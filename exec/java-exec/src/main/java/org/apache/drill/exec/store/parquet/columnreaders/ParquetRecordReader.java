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
package org.apache.drill.exec.store.parquet.columnreaders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.parquet.ParquetReaderStats;
import org.apache.drill.exec.store.parquet.ParquetRecordWriter;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;

import com.google.common.collect.Lists;

public class ParquetRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReader.class);

  // this value has been inflated to read in multiple value vectors at once, and then break them up into smaller vectors
  private static final int NUMBER_OF_VECTORS = 1;
  private static final long DEFAULT_BATCH_LENGTH = 256 * 1024 * NUMBER_OF_VECTORS; // 256kb
  private static final long DEFAULT_BATCH_LENGTH_IN_BITS = DEFAULT_BATCH_LENGTH * 8; // 256kb
  private static final char DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH = 32*1024;

  // TODO - should probably find a smarter way to set this, currently 1 megabyte
  public static final int PARQUET_PAGE_MAX_SIZE = 1024 * 1024 * 1;

  // used for clearing the last n bits of a byte
  public static final byte[] endBitMasks = {-2, -4, -8, -16, -32, -64, -128};
  // used for clearing the first n bits of a byte
  public static final byte[] startBitMasks = {127, 63, 31, 15, 7, 3, 1};

  private int bitWidthAllFixedFields;
  private boolean allFieldsFixedLength;
  private int recordsPerBatch;
  private OperatorContext operatorContext;
//  private long totalRecords;
//  private long rowGroupOffset;

  private List<ColumnReader<?>> columnStatuses;
  private FileSystem fileSystem;
  private long batchSize;
  Path hadoopPath;
  private VarLenBinaryReader varLengthReader;
  private ParquetMetadata footer;
  // This is a parallel list to the columns list above, it is used to determine the subset of the project
  // pushdown columns that do not appear in this file
  private boolean[] columnsFound;
  // For columns not found in the file, we need to return a schema element with the correct number of values
  // at that position in the schema. Currently this requires a vector be present. Here is a list of all of these vectors
  // that need only have their value count set at the end of each call to next(), as the values default to null.
  private List<NullableIntVector> nullFilledVectors;
  // Keeps track of the number of records returned in the case where only columns outside of the file were selected.
  // No actual data needs to be read out of the file, we only need to return batches until we have 'read' the number of
  // records specified in the row group metadata
  long mockRecordsRead;

  private final CodecFactory codecFactory;
  int rowGroupIndex;
  long totalRecordsRead;
  private final FragmentContext fragmentContext;
  private final boolean containsOldCorruptDates;

  public ParquetReaderStats parquetReaderStats = new ParquetReaderStats();

  public ParquetRecordReader(FragmentContext fragmentContext,
      String path,
      int rowGroupIndex,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns) throws ExecutionSetupException {
    this(fragmentContext, DEFAULT_BATCH_LENGTH_IN_BITS, path, rowGroupIndex, fs, codecFactory, footer,
        columns);
  }

  public ParquetRecordReader(
      FragmentContext fragmentContext,
      long batchSize,
      String path,
      int rowGroupIndex,
      FileSystem fs,
      CodecFactory codecFactory,
      ParquetMetadata footer,
      List<SchemaPath> columns) throws ExecutionSetupException {
    this.hadoopPath = new Path(path);
    this.fileSystem = fs;
    this.codecFactory = codecFactory;
    this.rowGroupIndex = rowGroupIndex;
    this.batchSize = batchSize;
    this.footer = footer;
    containsOldCorruptDates = detectCorruptDates(footer, columns);
    this.fragmentContext = fragmentContext;
    setColumns(columns);
  }

  private boolean detectCorruptDates(ParquetMetadata footer, List<SchemaPath> columns) {

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
          for (SchemaPath schemaPath : columns) {
            // this reader only supports flat data, this is restricted in the ParquetScanBatchCreator
//              for (int i = 0; i < columns.size(); ++i) {
//                ColumnDescriptor column = columns.get(i);
          }
//          for (int i = 0; i < columns.size(); ++i) {
//            ColumnDescriptor column = columns.get(i);
//            SchemaElement se = schemaElements.get(column.getPath()[0]);
//            MajorType mt = ParquetToDrillTypeConverter.toMajorType(column.getType(), se.getType_length(),
//                getDataMode(column), se, fragmentContext.getOptions());
//            field = MaterializedField.create(toFieldName(column.getPath()),mt);
//            if ( ! fieldSelected(field)) {
//              continue;
//            }
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
        System.out.println(parsedDrillVersion.application);
        if (parsedDrillVersion.application.equals("drill")) {
          SemanticVersion semVer = parsedDrillVersion.getSemanticVersion();
          if (semVer.major == 1 && semVer.minor < 5) {
            corruptDates = true;
          } else {
            System.out.println("dates not corrupt");
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

  /**
   * Flag indicating if the old non-standard data format appears
   * in this file, see DRILL-4203.
   *
   * @return true if the dates are corrupted and need to be corrected
   */
  public boolean containsOldCorruptDates() {
    return containsOldCorruptDates;
  }

  public CodecFactory getCodecFactory() {
    return codecFactory;
  }

  public Path getHadoopPath() {
    return hadoopPath;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public int getRowGroupIndex() {
    return rowGroupIndex;
  }

  public int getBitWidthAllFixedFields() {
    return bitWidthAllFixedFields;
  }

  public long getBatchSize() {
    return batchSize;
  }

  /**
   * @param type a fixed length type from the parquet library enum
   * @return the length in pageDataByteArray of the type
   */
  public static int getTypeLengthInBits(PrimitiveType.PrimitiveTypeName type) {
    switch (type) {
      case INT64:   return 64;
      case INT32:   return 32;
      case BOOLEAN: return 1;
      case FLOAT:   return 32;
      case DOUBLE:  return 64;
      case INT96:   return 96;
      // binary and fixed length byte array
      default:
        throw new IllegalStateException("Length cannot be determined for type " + type);
    }
  }

  private boolean fieldSelected(MaterializedField field) {
    // TODO - not sure if this is how we want to represent this
    // for now it makes the existing tests pass, simply selecting
    // all available data if no columns are provided
    if (isStarQuery()) {
      return true;
    }

    int i = 0;
    for (SchemaPath expr : getColumns()) {
      if ( field.matches(expr)) {
        columnsFound[i] = true;
        return true;
      }
      i++;
    }
    return false;
  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  /**
   * Returns data type length for a given {@see ColumnDescriptor} and it's corresponding
   * {@see SchemaElement}. Neither is enough information alone as the max
   * repetition level (indicating if it is an array type) is in the ColumnDescriptor and
   * the length of a fixed width field is stored at the schema level.
   *
   * @param column
   * @param se
   * @return the length if fixed width, else -1
   */
  private int getDataTypeLength(ColumnDescriptor column, SchemaElement se) {
    if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
      if (column.getMaxRepetitionLevel() > 0) {
        return -1;
      }
      if (column.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        return se.getType_length() * 8;
      } else {
        return getTypeLengthInBits(column.getType());
      }
    } else {
      return -1;
    }
  }

  @Override
  public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = operatorContext;
    if (!isStarQuery()) {
      columnsFound = new boolean[getColumns().size()];
      nullFilledVectors = new ArrayList<>();
    }
    columnStatuses = new ArrayList<>();
//    totalRecords = footer.getBlocks().get(rowGroupIndex).getRowCount();
    List<ColumnDescriptor> columns = footer.getFileMetaData().getSchema().getColumns();
    allFieldsFixedLength = true;
    ColumnDescriptor column;
    ColumnChunkMetaData columnChunkMetaData;
    int columnsToScan = 0;
    mockRecordsRead = 0;

    MaterializedField field;
//    ParquetMetadataConverter metaConverter = new ParquetMetadataConverter();
    FileMetaData fileMetaData;

    logger.debug("Reading row group({}) with {} records in file {}.", rowGroupIndex, footer.getBlocks().get(rowGroupIndex).getRowCount(),
        hadoopPath.toUri().getPath());
    totalRecordsRead = 0;

    // TODO - figure out how to deal with this better once we add nested reading, note also look where this map is used below
    // store a map from column name to converted types if they are non-null
    HashMap<String, SchemaElement> schemaElements = new HashMap<>();
    fileMetaData = new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    for (SchemaElement se : fileMetaData.getSchema()) {
      schemaElements.put(se.getName(), se);
    }

    // loop to add up the length of the fixed width columns and build the schema
    for (int i = 0; i < columns.size(); ++i) {
      column = columns.get(i);
      logger.debug("name: " + fileMetaData.getSchema().get(i).name);
      SchemaElement se = schemaElements.get(column.getPath()[0]);
      MajorType mt = ParquetToDrillTypeConverter.toMajorType(column.getType(), se.getType_length(),
          getDataMode(column), se, fragmentContext.getOptions());
      field = MaterializedField.create(toFieldName(column.getPath()),mt);
      if ( ! fieldSelected(field)) {
        continue;
      }
      columnsToScan++;
      int dataTypeLength = getDataTypeLength(column, se);
      if (dataTypeLength == -1) {
        allFieldsFixedLength = false;
      } else {
        bitWidthAllFixedFields += dataTypeLength;
      }
    }
//    rowGroupOffset = footer.getBlocks().get(rowGroupIndex).getColumns().get(0).getFirstDataPageOffset();

    if (columnsToScan != 0  && allFieldsFixedLength) {
      recordsPerBatch = (int) Math.min(Math.min(batchSize / bitWidthAllFixedFields,
          footer.getBlocks().get(0).getColumns().get(0).getValueCount()), 65535);
    }
    else {
      recordsPerBatch = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;
    }

    try {
      ValueVector vector;
      SchemaElement schemaElement;
      final ArrayList<VarLengthColumn> varLengthColumns = new ArrayList<>();
      // initialize all of the column read status objects
      boolean fieldFixedLength;
      // the column chunk meta-data is not guaranteed to be in the same order as the columns in the schema
      // a map is constructed for fast access to the correct columnChunkMetadata to correspond
      // to an element in the schema
      Map<String, Integer> columnChunkMetadataPositionsInList = new HashMap();
      BlockMetaData rowGroupMetadata = footer.getBlocks().get(rowGroupIndex);

      int colChunkIndex = 0;
      for (ColumnChunkMetaData colChunk : rowGroupMetadata.getColumns()) {
        columnChunkMetadataPositionsInList.put(Arrays.toString(colChunk.getPath().toArray()), colChunkIndex);
        colChunkIndex++;
      }
      for (int i = 0; i < columns.size(); ++i) {
        column = columns.get(i);
        columnChunkMetaData = rowGroupMetadata.getColumns().get(columnChunkMetadataPositionsInList.get(Arrays.toString(column.getPath())));
        schemaElement = schemaElements.get(column.getPath()[0]);
        MajorType type = ParquetToDrillTypeConverter.toMajorType(column.getType(), schemaElement.getType_length(),
            getDataMode(column), schemaElement, fragmentContext.getOptions());
        field = MaterializedField.create(toFieldName(column.getPath()), type);
        // the field was not requested to be read
        if ( ! fieldSelected(field)) {
          continue;
        }

        fieldFixedLength = column.getType() != PrimitiveType.PrimitiveTypeName.BINARY;
        vector = output.addField(field, (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()));
        if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
          if (column.getMaxRepetitionLevel() > 0) {
            final RepeatedValueVector repeatedVector = RepeatedValueVector.class.cast(vector);
            ColumnReader dataReader = ColumnReaderFactory.createFixedColumnReader(this, fieldFixedLength,
                column, columnChunkMetaData, recordsPerBatch,
                repeatedVector.getDataVector(), schemaElement);
            varLengthColumns.add(new FixedWidthRepeatedReader(this, dataReader,
                getTypeLengthInBits(column.getType()), -1, column, columnChunkMetaData, false, repeatedVector, schemaElement));
          }
          else {
            columnStatuses.add(ColumnReaderFactory.createFixedColumnReader(this, fieldFixedLength,
                column, columnChunkMetaData, recordsPerBatch, vector,
                schemaElement));
          }
        } else {
          // create a reader and add it to the appropriate list
          varLengthColumns.add(ColumnReaderFactory.getReader(this, -1, column, columnChunkMetaData, false, vector, schemaElement));
        }
      }
      varLengthReader = new VarLenBinaryReader(this, varLengthColumns);

      if (!isStarQuery()) {
        List<SchemaPath> projectedColumns = Lists.newArrayList(getColumns());
        SchemaPath col;
        for (int i = 0; i < columnsFound.length; i++) {
          col = projectedColumns.get(i);
          assert col!=null;
          if ( ! columnsFound[i] && !col.equals(STAR_COLUMN)) {
            nullFilledVectors.add((NullableIntVector)output.addField(MaterializedField.create(col,
                    Types.optional(TypeProtos.MinorType.INT)),
                (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(TypeProtos.MinorType.INT, DataMode.OPTIONAL)));

          }
        }
      }
    } catch (Exception e) {
      handleAndRaise("Failure in setting up reader", e);
    }
  }

  protected void handleAndRaise(String s, Exception e) {
    String message = "Error in parquet record reader.\nMessage: " + s +
      "\nParquet Metadata: " + footer;
    throw new DrillRuntimeException(message, e);
  }

  @Override
  public void allocate(Map<Key, ValueVector> vectorMap) throws OutOfMemoryException {
    try {
      for (final ValueVector v : vectorMap.values()) {
        AllocationHelper.allocate(v, recordsPerBatch, 50, 10);
      }
    } catch (NullPointerException e) {
      throw new OutOfMemoryException();
    }
  }


  private SchemaPath toFieldName(String[] paths) {
    return SchemaPath.getCompoundPath(paths);
  }

  private TypeProtos.DataMode getDataMode(ColumnDescriptor column) {
    if (column.getMaxRepetitionLevel() > 0 ) {
      return DataMode.REPEATED;
    } else if (column.getMaxDefinitionLevel() == 0) {
      return TypeProtos.DataMode.REQUIRED;
    } else {
      return TypeProtos.DataMode.OPTIONAL;
    }
  }

  private void resetBatch() {
    for (final ColumnReader<?> column : columnStatuses) {
      column.valuesReadInCurrentPass = 0;
    }
    for (final VarLengthColumn<?> r : varLengthReader.columns) {
      r.valuesReadInCurrentPass = 0;
    }
  }

 public void readAllFixedFields(long recordsToRead) throws IOException {

   for (ColumnReader<?> crs : columnStatuses) {
     crs.processPages(recordsToRead);
   }
 }

  @Override
  public int next() {
    resetBatch();
    long recordsToRead = 0;
    try {
      ColumnReader<?> firstColumnStatus;
      if (columnStatuses.size() > 0) {
        firstColumnStatus = columnStatuses.iterator().next();
      }
      else{
        if (varLengthReader.columns.size() > 0) {
          firstColumnStatus = varLengthReader.columns.iterator().next();
        }
        else{
          firstColumnStatus = null;
        }
      }
      // No columns found in the file were selected, simply return a full batch of null records for each column requested
      if (firstColumnStatus == null) {
        if (mockRecordsRead == footer.getBlocks().get(rowGroupIndex).getRowCount()) {
          return 0;
        }
        recordsToRead = Math.min(DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH, footer.getBlocks().get(rowGroupIndex).getRowCount() - mockRecordsRead);
        for (final ValueVector vv : nullFilledVectors ) {
          vv.getMutator().setValueCount( (int) recordsToRead);
        }
        mockRecordsRead += recordsToRead;
        totalRecordsRead += recordsToRead;
        return (int) recordsToRead;
      }

      if (allFieldsFixedLength) {
        recordsToRead = Math.min(recordsPerBatch, firstColumnStatus.columnChunkMetaData.getValueCount() - firstColumnStatus.totalValuesRead);
      } else {
        recordsToRead = DEFAULT_RECORDS_TO_READ_IF_NOT_FIXED_WIDTH;

      }

      if (allFieldsFixedLength) {
        readAllFixedFields(recordsToRead);
      } else { // variable length columns
        long fixedRecordsToRead = varLengthReader.readFields(recordsToRead, firstColumnStatus);
        readAllFixedFields(fixedRecordsToRead);
      }

      // if we have requested columns that were not found in the file fill their vectors with null
      // (by simply setting the value counts inside of them, as they start null filled)
      if (nullFilledVectors != null) {
        for (final ValueVector vv : nullFilledVectors ) {
          vv.getMutator().setValueCount(firstColumnStatus.getRecordsReadInCurrentPass());
        }
      }

//      logger.debug("So far read {} records out of row group({}) in file '{}'", totalRecordsRead, rowGroupIndex, hadoopPath.toUri().getPath());
      totalRecordsRead += firstColumnStatus.getRecordsReadInCurrentPass();
      return firstColumnStatus.getRecordsReadInCurrentPass();
    } catch (Exception e) {
      handleAndRaise("\nHadoop path: " + hadoopPath.toUri().getPath() +
        "\nTotal records read: " + totalRecordsRead +
        "\nMock records read: " + mockRecordsRead +
        "\nRecords to read: " + recordsToRead +
        "\nRow group index: " + rowGroupIndex +
        "\nRecords in row group: " + footer.getBlocks().get(rowGroupIndex).getRowCount(), e);
    }

    // this is never reached
    return 0;
  }

  @Override
  public void close() {
    logger.debug("Read {} records out of row group({}) in file '{}'", totalRecordsRead, rowGroupIndex, hadoopPath.toUri().getPath());
    // enable this for debugging when it is know that a whole file will be read
    // limit kills upstream operators once it has enough records, so this assert will fail
//    assert totalRecordsRead == footer.getBlocks().get(rowGroupIndex).getRowCount();
    if (columnStatuses != null) {
      for (final ColumnReader column : columnStatuses) {
        column.clear();
      }
      columnStatuses.clear();
      columnStatuses = null;
    }

    codecFactory.release();

    if (varLengthReader != null) {
      for (final VarLengthColumn r : varLengthReader.columns) {
        r.clear();
      }
      varLengthReader.columns.clear();
      varLengthReader = null;
    }

    if(parquetReaderStats != null) {
      logger.trace("ParquetTrace,Summary,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
          hadoopPath,
          parquetReaderStats.numDictPageHeaders,
          parquetReaderStats.numPageHeaders,
          parquetReaderStats.numDictPageLoads,
          parquetReaderStats.numPageLoads,
          parquetReaderStats.numDictPagesDecompressed,
          parquetReaderStats.numPagesDecompressed,
          parquetReaderStats.totalDictPageHeaderBytes,
          parquetReaderStats.totalPageHeaderBytes,
          parquetReaderStats.totalDictPageReadBytes,
          parquetReaderStats.totalPageReadBytes,
          parquetReaderStats.totalDictDecompressedBytes,
          parquetReaderStats.totalDecompressedBytes,
          parquetReaderStats.timeDictPageHeaders,
          parquetReaderStats.timePageHeaders,
          parquetReaderStats.timeDictPageLoads,
          parquetReaderStats.timePageLoads,
          parquetReaderStats.timeDictPagesDecompressed,
          parquetReaderStats.timePagesDecompressed);
      parquetReaderStats=null;
    }
  }
}
