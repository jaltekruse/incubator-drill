/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.store.parquet2.DrillParquetReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


public class ParquetScanBatchCreator implements BatchCreator<ParquetRowGroupScan>{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetScanBatchCreator.class);

  private static final String ENABLE_BYTES_READ_COUNTER = "parquet.benchmark.bytes.read";
  private static final String ENABLE_BYTES_TOTAL_COUNTER = "parquet.benchmark.bytes.total";
  private static final String ENABLE_TIME_READ_COUNTER = "parquet.benchmark.time.read";

  @Override
  public ScanBatch getBatch(FragmentContext context, ParquetRowGroupScan rowGroupScan, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    String partitionDesignator = context.getOptions()
      .getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val;
    List<SchemaPath> columns = rowGroupScan.getColumns();
    List<RecordReader> readers = Lists.newArrayList();
    OperatorContext oContext = context.newOperatorContext(rowGroupScan);

    List<String[]> partitionColumns = Lists.newArrayList();
    List<Integer> selectedPartitionColumns = Lists.newArrayList();
    boolean selectAllColumns = AbstractRecordReader.isStarQuery(columns);

    List<SchemaPath> newColumns = columns;
    if (!selectAllColumns) {
      newColumns = Lists.newArrayList();
      Pattern pattern = Pattern.compile(String.format("%s[0-9]+", partitionDesignator));
      for (SchemaPath column : columns) {
        Matcher m = pattern.matcher(column.getAsUnescapedPath());
        if (m.matches()) {
          selectedPartitionColumns.add(Integer.parseInt(column.getAsUnescapedPath().toString().substring(partitionDesignator.length())));
        } else {
          newColumns.add(column);
        }
      }
      if (newColumns.isEmpty()) {
        newColumns = GroupScan.ALL_COLUMNS;
      }
      final int id = rowGroupScan.getOperatorId();
      // Create the new row group scan with the new columns
      rowGroupScan = new ParquetRowGroupScan(rowGroupScan.getUserName(), rowGroupScan.getStorageEngine(),
          rowGroupScan.getRowGroupReadEntries(), newColumns, rowGroupScan.getSelectionRoot());
      rowGroupScan.setOperatorId(id);
    }

    DrillFileSystem fs;
    try {
      fs = oContext.newFileSystem(rowGroupScan.getStorageEngine().getFsConf());
    } catch(IOException e) {
      throw new ExecutionSetupException(String.format("Failed to create DrillFileSystem: %s", e.getMessage()), e);
    }
    Configuration conf = new Configuration(fs.getConf());
    conf.setBoolean(ENABLE_BYTES_READ_COUNTER, false);
    conf.setBoolean(ENABLE_BYTES_TOTAL_COUNTER, false);
    conf.setBoolean(ENABLE_TIME_READ_COUNTER, false);

    // keep footers in a map to avoid re-reading them
    Map<String, ParquetMetadata> footers = new HashMap<String, ParquetMetadata>();
    int numParts = 0;
    for(RowGroupReadEntry e : rowGroupScan.getRowGroupReadEntries()){
      /*
      Here we could store a map from file names to footers, to prevent re-reading the footer for each row group in a file
      TODO - to prevent reading the footer again in the parquet record reader (it is read earlier in the ParquetStorageEngine)
      we should add more information to the RowGroupInfo that will be populated upon the first read to
      provide the reader with all of th file meta-data it needs
      These fields will be added to the constructor below
      */
      try {
        Stopwatch timer = new Stopwatch();
        if ( ! footers.containsKey(e.getPath())){
          timer.start();
          ParquetMetadata footer = ParquetFileReader.readFooter(conf, new Path(e.getPath()));
          long timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
          logger.trace("ParquetTrace,Read Footer,{},{},{},{},{},{},{}", "", e.getPath(), "", 0, 0, 0, timeToRead);
          footers.put(e.getPath(), footer );
        }
        boolean containsCorruptDates = detectCorruptDates(footers.get(e.getPath()), rowGroupScan.getColumns(), e.getRowGroupIndex());
        if (!context.getOptions().getOption(ExecConstants.PARQUET_NEW_RECORD_READER).bool_val && !isComplex(footers.get(e.getPath()))) {
          readers.add(
              new ParquetRecordReader(
                  context, e.getPath(), e.getRowGroupIndex(), fs,
                  CodecFactory.createDirectCodecFactory(
                  fs.getConf(),
                  new ParquetDirectByteBufferAllocator(oContext.getAllocator()), 0),
                  footers.get(e.getPath()),
                  rowGroupScan.getColumns(),
                  containsCorruptDates
              )
          );
        } else {
          ParquetMetadata footer = footers.get(e.getPath());
          readers.add(new DrillParquetReader(context, footer, e, newColumns, fs, containsCorruptDates));
        }
        if (rowGroupScan.getSelectionRoot() != null) {
          String[] r = Path.getPathWithoutSchemeAndAuthority(new Path(rowGroupScan.getSelectionRoot())).toString().split("/");
          String[] p = Path.getPathWithoutSchemeAndAuthority(new Path(e.getPath())).toString().split("/");
          if (p.length > r.length) {
            String[] q = ArrayUtils.subarray(p, r.length, p.length - 1);
            partitionColumns.add(q);
            numParts = Math.max(numParts, q.length);
          } else {
            partitionColumns.add(new String[] {});
          }
        } else {
          partitionColumns.add(new String[] {});
        }
      } catch (IOException e1) {
        throw new ExecutionSetupException(e1);
      }
    }

    if (selectAllColumns) {
      for (int i = 0; i < numParts; i++) {
        selectedPartitionColumns.add(i);
      }
    }

    ScanBatch s =
        new ScanBatch(rowGroupScan, context, oContext, readers.iterator(), partitionColumns, selectedPartitionColumns);


    return s;
  }

  private static boolean isComplex(ParquetMetadata footer) {
    MessageType schema = footer.getFileMetaData().getSchema();

    for (Type type : schema.getFields()) {
      if (!type.isPrimitive()) {
        return true;
      }
    }
    for (ColumnDescriptor col : schema.getColumns()) {
      if (col.getMaxRepetitionLevel() > 0) {
        return true;
      }
    }
    return false;
  }



  private boolean detectCorruptDates(ParquetMetadata footer, List<SchemaPath> columns, int rowGroupIndex) {

    // old drill files have parquet-mr, no drill version, need to check min/max values to see if they look corrupt
    //  - option to disable this auto-correction based on the date values, in case users are storing these dates intentionally

    // migrated parquet files have 1.8.1 parquet-mr version with drill-r0 in the part of the name usually containing "SNAPSHOT"

    // new parquet files 1.4 and 1.5 have drill version number
    //  - below 1.5 dates are corrupt
    //  - this includes 1.5 SNAPSHOT

    String drillVersion = footer.getFileMetaData().getKeyValueMetaData().get(ParquetRecordWriter.DRILL_VERSION_PROPERTY);
    String createdBy = footer.getFileMetaData().getCreatedBy();
    boolean corruptDates = false;
    try {
      if (drillVersion == null) {
        // Possibly an old, un-migrated Drill file, check the column statistics to see if min/max values look corrupt
        // only applies if there is a date column selected
        if (createdBy.equals("parquet-mr")) {
          Map<String, SchemaElement> schemaElements = ParquetReaderUtility.getColNameToSchemaElementMapping(footer);
          findDateColWithStatsLoop : for (SchemaPath schemaPath : columns) {
            List<ColumnDescriptor> parquetColumns = footer.getFileMetaData().getSchema().getColumns();
            for (int i = 0; i < parquetColumns.size(); ++i) {
              ColumnDescriptor column = parquetColumns.get(i);
              // this reader only supports flat data, this is restricted in the ParquetScanBatchCreator
              // creating a NameSegment makes sure we are using the standard code for comparing names,
              // currently it is all case-insensitive
              if (AbstractRecordReader.isStarQuery(columns) || new PathSegment.NameSegment(column.getPath()[0]).equals(schemaPath.getRootSegment())) {
                int colIndex = -1;
                if (schemaElements.get(column.getPath()[0]).getConverted_type().equals(ConvertedType.DATE)) {
                  List<ColumnChunkMetaData> colChunkList = footer.getBlocks().get(rowGroupIndex).getColumns();
                  for (int j = 0; j < colChunkList.size(); j++) {
                    if (colChunkList.get(j).getPath().equals(ColumnPath.get(column.getPath()))) {
                      colIndex = j;
                      break;
                    }
                  }
                }
                Preconditions.checkArgument(colIndex != -1, "Issue reading parquet metadata");
                Statistics statistics = footer.getBlocks().get(rowGroupIndex).getColumns().get(colIndex).getStatistics();
                Integer max = (Integer) statistics.genericGetMax();
                if (max != null && max > 1_000_00) {
                  corruptDates = true;
                  break findDateColWithStatsLoop;
                }
              }
            }
          }
        } else {
          // check the created by to see if it is a migrated Drill file
          VersionParser.ParsedVersion parsedCreatedByVersion = VersionParser.parse(createdBy);
          // check if this is a migrated Drill file, lacking a Drill version number, but with
          // "drill" in the parquet created-by string
          SemanticVersion semVer = parsedCreatedByVersion.getSemanticVersion();
          if (semVer.major == 1 && semVer.minor == 8 && semVer.patch == 1 && semVer.unknown.contains("drill")) {
            corruptDates = true;
          }
        }
      } else {
        // this parser expects an application name before the semantic version, just prepending Drill
        // we know from the property name "drill.version" that we wrote this
        VersionParser.ParsedVersion parsedDrillVersion = VersionParser.parse("drill version " + drillVersion + " (build 1234)");
        if (parsedDrillVersion.application.equals("drill")) {
          SemanticVersion semVer = parsedDrillVersion.getSemanticVersion();
          if (semVer.compareTo(new SemanticVersion(1, 5, 0)) < 0) {
            corruptDates = true;
          } else {
            corruptDates = false;
          }
        } else { // Drill has always included parquet-mr as the application name in the file metadata
          corruptDates = false;
        }
      }
    } catch (VersionParser.VersionParseException e) {
      // Default value of "false" if we cannot parse the version is fine, we are covering all
      // of the metadata values produced by historical versions of Drill
      // If Drill didn't write it the dates should be fine
    }
    return corruptDates;
  }

}
