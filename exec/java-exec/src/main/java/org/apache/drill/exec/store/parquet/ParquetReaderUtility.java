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
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
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
import org.apache.parquet.schema.OriginalType;

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

  public static int autoCorrectCorruptedDate(int corruptedDate) {
    return (int) (corruptedDate - 2 * ParquetOutputRecordWriter.JULIAN_DAY_EPOC);
  }

  public static void correctDatesInMetadataCache(Metadata.ParquetTableMetadataBase parquetTableMetadata) {
    // TODO - replace with check for Drill version number
    boolean cacheFileContainsCorruptDates;
    String drillVersionStr = parquetTableMetadata.getDrillVersion();
    if (drillVersionStr != null) {
      try {
        cacheFileContainsCorruptDates = ParquetReaderUtility.drillVersionHasCorruptedDates(drillVersionStr);
      } catch (VersionParser.VersionParseException e) {
        cacheFileContainsCorruptDates = true;
      }
    } else {
      cacheFileContainsCorruptDates = true;
    }
    if (cacheFileContainsCorruptDates) {
      for (Metadata.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        // Drill has only ever written a single row group per file, only need to correct the statistics
        // on the first row group
        Metadata.RowGroupMetadata rowGroupMetadata = file.getRowGroups().get(0);
        for (Metadata.ColumnMetadata columnMetadata : rowGroupMetadata.getColumns()) {
          OriginalType originalType = columnMetadata.getOriginalType();
          if (originalType != null && originalType.equals(OriginalType.DATE) &&
              columnMetadata.hasSingleValue() &&
              (Integer) columnMetadata.getMaxValue() > 1_000_000) {
            int newMinMax = ParquetReaderUtility.autoCorrectCorruptedDate((Integer)columnMetadata.getMaxValue());
            columnMetadata.setMax(newMinMax);
            columnMetadata.setMin(newMinMax);
          }
        }
      }
    }
  }

  /**
   * Check for corrupted dates in a parquet file. See Drill-4203
   * @param footer
   * @param columns
   * @return
   */
  public static boolean detectCorruptDates(ParquetMetadata footer,
                                           List<SchemaPath> columns,
                                           boolean autoCorrectCorruptDates) {
    // old drill files have "parquet-mr" as created by string, and no drill version, need to check min/max values to see
    // if they look corrupt
    //  - option to disable this auto-correction based on the date values, in case users are storing these
    //    dates intentionally

    // migrated parquet files have 1.8.1 parquet-mr version with drill-r0 in the part of the name usually containing "SNAPSHOT"

    // new parquet files 1.4 and 1.5 have drill version number
    //  - below 1.5 dates are corrupt
    //  - this includes 1.5 SNAPSHOT

    String drillVersion = footer.getFileMetaData().getKeyValueMetaData().get(ParquetRecordWriter.DRILL_VERSION_PROPERTY);
    String createdBy = footer.getFileMetaData().getCreatedBy();
    try {
      if (drillVersion == null) {
        // Possibly an old, un-migrated Drill file, check the column statistics to see if min/max values look corrupt
        // only applies if there is a date column selected
        if (createdBy.equals("parquet-mr")) {
          // loop through parquet column metadata to find date columns, check for corrupt valuues
          return checkForCorruptDateValuesInStatistics(footer, columns, autoCorrectCorruptDates);
        } else {
          // check the created by to see if it is a migrated Drill file
          VersionParser.ParsedVersion parsedCreatedByVersion = VersionParser.parse(createdBy);
          // check if this is a migrated Drill file, lacking a Drill version number, but with
          // "drill" in the parquet created-by string
          SemanticVersion semVer = parsedCreatedByVersion.getSemanticVersion();
          if (semVer != null && semVer.major == 1 && semVer.minor == 8 && semVer.patch == 1 && semVer.unknown.contains("drill")) {
            return true;
          } else {
            // written by a tool that wasn't Drill, the dates are not corrupted
            return false;
          }
        }
      } else {
        // this parser expects an application name before the semantic version, just prepending Drill
        // we know from the property name "drill.version" that we wrote this
        return drillVersionHasCorruptedDates(drillVersion);
      }
    } catch (VersionParser.VersionParseException e) {
      // Default value of "false" if we cannot parse the version is fine, we are covering all
      // of the metadata values produced by historical versions of Drill
      // If Drill didn't write it the dates should be fine
      return false;
    }
  }

  public static boolean drillVersionHasCorruptedDates(String drillVersion) throws VersionParser.VersionParseException {
    VersionParser.ParsedVersion parsedDrillVersion = parseDrillVersion(drillVersion);
    SemanticVersion semVer = parsedDrillVersion.getSemanticVersion();
    if (semVer == null || semVer.compareTo(new SemanticVersion(1, 5, 0)) < 0) {
      return true;
    } else {
      return false;
    }

  }

  public static VersionParser.ParsedVersion parseDrillVersion(String drillVersion) throws VersionParser.VersionParseException {
    return VersionParser.parse("drill version " + drillVersion + " (build 1234)");
  }

  /**
   * Detect corrupt date values by looking at the min/max values in the metadata.
   *
   * This should only be used when a file does not have enough metadata to determine if
   * the data was written with an older version of Drill, or an external tool. Drill
   * versions 1.3 and beyond should have enough metadata to confirm that the data was written
   * by Drill.
   *
   * This method only checks the first Row Group, because Drill has only ever written
   * a single Row Group per file.
   *
   * @param footer
   * @param columns
   * @param autoCorrectCorruptDates user setting to allow enabling/disabling of auto-correction
   *                                of corrupt dates. There are some rare cases (storing dates hundreds
   *                                of years into the future, with tools other than Drill writing files)
   *                                that would result in the date values being "corrected" into bad values.
   * @return
   */
  public static boolean checkForCorruptDateValuesInStatistics(ParquetMetadata footer,
                                                              List<SchemaPath> columns,
                                                              boolean autoCorrectCorruptDates) {
    // Users can turn-off date correction in cases where we are detecting corruption based on the date values
    // that are unlikely to appear in common datasets. In this case report that no correction needs to happen
    // during the file read
    if (! autoCorrectCorruptDates) {
      return false;
    }
    // Drill produced files have only ever have a single row group, if this changes in the future it won't matter
    // as we will know from the Drill version written in the files that the dates are correct
    int rowGroupIndex = 0;
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
          ConvertedType convertedType = schemaElements.get(column.getPath()[0]).getConverted_type();
          if (convertedType != null && convertedType.equals(ConvertedType.DATE)) {
            List<ColumnChunkMetaData> colChunkList = footer.getBlocks().get(rowGroupIndex).getColumns();
            for (int j = 0; j < colChunkList.size(); j++) {
              if (colChunkList.get(j).getPath().equals(ColumnPath.get(column.getPath()))) {
                colIndex = j;
                break;
              }
            }
          }
          if (colIndex == -1) {
            // column does not appear in this file, skip it
            continue;
          }
          Statistics statistics = footer.getBlocks().get(rowGroupIndex).getColumns().get(colIndex).getStatistics();
          Integer max = (Integer) statistics.genericGetMax();
          // TODO - make sure this threshold is set well
          if (statistics.hasNonNullValue() && max > 1_000_000) {
            return true;
          }
        }
      }
    }
    return false;
  }
}
