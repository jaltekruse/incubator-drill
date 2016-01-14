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

import com.google.common.base.Preconditions;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.work.ExecErrorConstants;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Utility class where we can capture common logic between the two parquet readers
 */
public class ParquetReaderUtility {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetReaderUtility.class);

  public static void checkDecimalTypeEnabled(OptionManager options) {
    if (options.getOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY).bool_val == false) {
      throw UserException.unsupportedError()
        .message(ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG)
        .build(logger);
    }
  }

  public static int getIntFromLEBytes(byte[] input, int start) {
    int out = 0;
    int shiftOrder = 0;
    for (int i = start; i < start + 4; i++) {
      out |= (((input[i]) & 0xFF) << shiftOrder);
      shiftOrder += 8;
    }
    return out;
  }

  public static Map<String, SchemaElement> getColNameToSchemaElementMapping(ParquetMetadata footer) {
    HashMap<String, SchemaElement> schemaElements = new HashMap<>();
    FileMetaData fileMetaData = new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    for (SchemaElement se : fileMetaData.getSchema()) {
      schemaElements.put(se.getName(), se);
    }
    return schemaElements;
  }

  public static boolean detectCorruptDates(ParquetMetadata footer, List<SchemaPath> columns, int rowGroupIndex) {

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
                // TODO - make sure this threshold is set well
                if (statistics.hasNonNullValue() && max > 1_000_000) {
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
