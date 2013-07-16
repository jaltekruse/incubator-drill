/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.store;

import com.beust.jcommander.internal.Maps;
import io.netty.buffer.ByteBuf;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.SchemaBuilder;
import org.apache.drill.exec.record.vector.TypeHelper;
import org.apache.drill.exec.record.vector.ValueVector;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ParquetRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRecordReader.class);
  private static final int DEFAULT_LENGTH_IN_BITS = 256 * 1024 * 8; // 256kb
  private static final String SEPERATOR = System.getProperty("file.separator");

  private ParquetFileReader parquetReader;
  private BatchSchema currentSchema;
  private int bitWidthAllFixedFields;
  private boolean allFieldsFixedLength;
  private int recordsPerBatch;

  // used for clearing the last n bits of a byte
  private byte[] bitMasks = { -2, -4, -8, -16, -32, -64, -128};

  // used for clearing the first n bits of a byte
  private byte[] startBitMasks = { 127, 63, 31, 15, 7, 3, 1};

  private class ColumnReadStatus {
    // Value Vector for this column
    VectorHolder valueVecHolder;
    // column description from the parquet library
    ColumnDescriptor columnDescriptor;
    // metadata of the column, from the parquet library
    ColumnChunkMetaData columnChunkMetaData;
    // status information on the current page
    PageReadStatus pageReadStatus;
    // quick reference to see if the field is fixed length (as this requires an instanceof)
    boolean isFixedLength;
    // counter for the total number of values read from one or more pages
    // when a batch is filled all of these values should be the same for each column
    int totalValuesRead;
    // counter for the values that have been read in this pass (a single call to the next() method)
    int valuesReadInCurrentPass;
    // length of single data value in bits
    int dataTypeLengthInBits;

  }

  // class to keep track of the read position of variable length columns
  private class PageReadStatus {
    // store references to the pages that have been uncompressed, but not copied to ValueVectors yet
    Page currentPage;

    PageReader pageReader;
    // read position in the current page, stored in bits, to keep track of boolean columns that did
    // not end on a byte boundary (currently only possible with booleans)
    long readPosInBits;
    // bit shift needed for the next page if the last one did not line up with a byte boundary
    int bitShift;
    // storage space for extra bits at the end of a page if they did not line up with a byte boundary
    // prevents the need to keep the entire last page, as these bytes need to be added to the next batch
    byte extraBits;
    // the number of values read out of the last page
    int valuesRead;

    public boolean next() {
      currentPage = pageReader.readPage();
      if (currentPage == null) {
        return false;
      }
      readPosInBits = 0;
      valuesRead = 0;
      return true;
    }
  }

  // this class represents a row group, it is named poorly in the parquet library
  private PageReadStore currentRowGroup;
  private Map<MaterializedField, ColumnReadStatus> columnStatuses;


  // would only need this to compare schemas of different row groups
  //List<Footer> footers;
  //Iterator<Footer> footerIter;
  ParquetMetadata footer;
  BytesInput currBytes;

  private OutputMutator outputMutator;
  private BufferAllocator allocator;
  private int currentRowGroupIndex;
  private int batchSize;
  private MessageType schema;


  public ParquetRecordReader(FragmentContext fragmentContext,
                             ParquetFileReader reader, ParquetMetadata footer) {
    this(fragmentContext, DEFAULT_LENGTH_IN_BITS, reader, footer);
  }


  public ParquetRecordReader(FragmentContext fragmentContext, int batchSize,
                             ParquetFileReader reader, ParquetMetadata footer) {
    this.allocator = fragmentContext.getAllocator();
    this.batchSize = batchSize;
    this.footer = footer;

    parquetReader = reader;
  }

  /**
   * @param type a fixed length type from the parquet library enum
   * @return the length in bytes of the type
   */
  public static int getTypeLengthInBytes(PrimitiveType.PrimitiveTypeName type) {
    switch (type) {
      case INT64:
        return 64;
      case INT32:
        return 32;
      case BOOLEAN:
        return 1;
      case FLOAT:
        return 32;
      case DOUBLE:
        return 64;
      case INT96:
        return 96;
      // binary, fixed length byte array
      default:
        throw new IllegalStateException("Length cannot be determined for type " + type);
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    outputMutator = output;
    schema = footer.getFileMetaData().getSchema();
    currentRowGroupIndex = -1;
    columnStatuses = Maps.newHashMap();
    currentRowGroup = null;

    List<ColumnDescriptor> columns = schema.getColumns();
    allFieldsFixedLength = true;
    ColumnDescriptor column = null;
    ColumnChunkMetaData columnChunkMetaData = null;
    SchemaBuilder builder = BatchSchema.newBuilder();
    boolean fieldFixed = false;

    // loop to add up the length of the fixed width columns and build the schema
    for (int i = 0; i < columns.size(); ++i) {
      column = columns.get(i);
      MaterializedField field = MaterializedField.create(new SchemaPath(toFieldName(column.getPath())),
          toMajorType(column.getType(), getDataMode(column)));

      // sum the lengths of all of the fixed length fields
      if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
        // There is not support for the fixed binary type yet in parquet, leaving a task here as a reminder
        // TODO - implement this when the feature is added upstream
//          if (column.getType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY){
//              byteWidthAllFixedFields += column.getType().getWidth()
//          }
//          else { } // the code below for the rest of the fixed length fields

        fieldFixed = true;
        bitWidthAllFixedFields += getTypeLengthInBytes(column.getType());
      } else {
        fieldFixed = false;
        allFieldsFixedLength = false;
      }

      builder.addField(field);
    }
    currentSchema = builder.build();

    if (allFieldsFixedLength) {
      recordsPerBatch = (int) Math.min(DEFAULT_LENGTH_IN_BITS / bitWidthAllFixedFields, footer.getBlocks().get(0).getColumns().get(0).getValueCount());
    }
    try {
      // initialize all of the column read status objects, if their lengths are known value vectors are allocated
      int i = 0;
      for (MaterializedField field : currentSchema) {
        column = columns.get(i);
        columnChunkMetaData = footer.getBlocks().get(0).getColumns().get(i);
        field = MaterializedField.create(new SchemaPath(toFieldName(column.getPath())),
            toMajorType(column.getType(), getDataMode(column)));
        if (allFieldsFixedLength) {
          createColumnStatus(fieldFixed, field, column, columnChunkMetaData, recordsPerBatch);
        }
        else{
          createColumnStatus(fieldFixed, field, column, columnChunkMetaData, -1);
        }
        i++;
      }
    } catch (SchemaChangeException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  private static String toFieldName(String[] paths) {
    return join(SEPERATOR, paths);
  }

  private SchemaDefProtos.DataMode getDataMode(ColumnDescriptor column) {
    if (schema.getColumnDescription(column.getPath()).getMaxDefinitionLevel() == 0) {
      return SchemaDefProtos.DataMode.REQUIRED;
    } else {
      return SchemaDefProtos.DataMode.OPTIONAL;
    }
  }

  private void resetBatch() {
    for (ColumnReadStatus column : columnStatuses.values()) {
      column.valueVecHolder.reset();
      column.valuesReadInCurrentPass = 0;
    }
  }

  /**
   * @param fixedLength
   * @param field
   * @param descriptor
   * @param columnChunkMetaData
   * @param allocateSize        - the size of the vector to create, if the value is less than 1 the vector is left null for variable length
   * @return
   * @throws SchemaChangeException
   */
  private boolean createColumnStatus(boolean fixedLength, MaterializedField field, ColumnDescriptor descriptor,
                                     ColumnChunkMetaData columnChunkMetaData, int allocateSize) throws SchemaChangeException {
    SchemaDefProtos.MajorType type = field.getType();
    ValueVector.Base v = TypeHelper.getNewVector(field, allocator);
    ColumnReadStatus newCol = new ColumnReadStatus();
    newCol.valueVecHolder = new VectorHolder(allocateSize, v);
    if (allocateSize > 1) {
      newCol.valueVecHolder.reset();
    }
    newCol.columnDescriptor = descriptor;
    newCol.columnChunkMetaData = columnChunkMetaData;
    newCol.isFixedLength = fixedLength;
    newCol.dataTypeLengthInBits = getTypeLengthInBytes(newCol.columnDescriptor.getType());
    columnStatuses.put(field, newCol);
    outputMutator.addField(0, v);
    return true;
  }

  public boolean checkBitShiftNeeded(long readLengthInBits, long readLength, ColumnReadStatus columnReadStatus,
                                     long currRecordsRead, byte[] bytes){
    if (readLengthInBits % 8 != 0){
      columnReadStatus.pageReadStatus.extraBits = bytes[bytes.length - 1];
      readLength--;
      columnReadStatus.pageReadStatus.bitShift = 8 - (int) readLengthInBits % 8;
      currRecordsRead -= readLengthInBits % 8;
      return true;
    }
    else{
      columnReadStatus.pageReadStatus.extraBits = 0;
      columnReadStatus.pageReadStatus.bitShift = 0;
      return false;
    }
  }

  public void readAllFixedFields(long recordsToRead) throws IOException {
    long readStartInBits = 0, readLength = 0, readLengthInBits = 0, currRecordsRead = 0;
    byte[] bytes;
    byte firstByte;
    byte currentByte;
    byte nextByte;
    ByteBuf buffer;
    for (ColumnReadStatus columnReadStatus : columnStatuses.values()) {
      if (columnReadStatus.pageReadStatus == null) {
        columnReadStatus.pageReadStatus = new PageReadStatus();
      }
      if (columnReadStatus.pageReadStatus.pageReader == null) {
        columnReadStatus.pageReadStatus.pageReader = currentRowGroup.getPageReader(columnReadStatus.columnDescriptor);
      }
      do {
        // if no page has been read, or all of the records have been read out of a page, read the next one
        if (columnReadStatus.pageReadStatus.currentPage == null
            || columnReadStatus.pageReadStatus.valuesRead == columnReadStatus.pageReadStatus.currentPage.getValueCount()) {
          columnReadStatus.totalValuesRead += columnReadStatus.pageReadStatus.valuesRead;
          if (!columnReadStatus.pageReadStatus.next()) {
            break;
          }
        }

        currRecordsRead =  Math.min( columnReadStatus.pageReadStatus.currentPage.getValueCount()
            - columnReadStatus.pageReadStatus.valuesRead, recordsToRead - columnReadStatus.valuesReadInCurrentPass);

        readStartInBits = columnReadStatus.pageReadStatus.readPosInBits;
        readLengthInBits = currRecordsRead * columnReadStatus.dataTypeLengthInBits;
        readLength = (int) Math.ceil(readLengthInBits / 8.0);
        currBytes = columnReadStatus.pageReadStatus.currentPage.getBytes();



        bytes = currBytes.toByteArray();
        // read in individual values, because a bitshift is necessary with where the last page ended
        if (columnReadStatus.pageReadStatus.bitShift != 0){
          buffer = columnReadStatus.valueVecHolder.getValueVector().data;
          // bit shift the leftover bits from the last read
          firstByte = (byte) (columnReadStatus.pageReadStatus.extraBits >>> columnReadStatus.pageReadStatus.bitShift);
          // mask the bits about to be added from the next byte
          firstByte = (byte) (firstByte & startBitMasks[columnReadStatus.pageReadStatus.bitShift - 1]);
          // grab the next byte from the buffer, shift and mask it, and OR it with the leftover bits
          nextByte = bytes[(int) Math.ceil(columnReadStatus.pageReadStatus.valuesRead / 8.0)];
          firstByte = (byte) (firstByte | nextByte
              << (8 - columnReadStatus.pageReadStatus.bitShift)
              & bitMasks[8 - columnReadStatus.pageReadStatus.bitShift - 1]);
          buffer.setByte(columnReadStatus.valuesReadInCurrentPass / 8, firstByte);
          readLengthInBits = currRecordsRead * columnReadStatus.dataTypeLengthInBits - columnReadStatus.pageReadStatus.bitShift;

          int i = 1;
          for ( ; i < (int) Math.ceil(readLengthInBits / 8.0); i++){
            currentByte = nextByte;
            currentByte = (byte) (currentByte >>> columnReadStatus.pageReadStatus.bitShift);
            // mask the bits about to be added from the next byte
            currentByte = (byte) (currentByte & startBitMasks[columnReadStatus.pageReadStatus.bitShift - 1]);
            if ( (int) Math.ceil(columnReadStatus.pageReadStatus.valuesRead / 8.0) + i < bytes.length){
              // grab the next byte from the buffer, shift and mask it, and OR it with the leftover bits
              nextByte = bytes[(int) Math.ceil(columnReadStatus.pageReadStatus.valuesRead / 8.0) + i];
              currentByte = (byte) (currentByte | nextByte
                  << (8 - columnReadStatus.pageReadStatus.bitShift)
                  & bitMasks[8 - columnReadStatus.pageReadStatus.bitShift - 1]);
            }
            buffer.setByte(columnReadStatus.valuesReadInCurrentPass / 8 + i, currentByte);
          }
          if (readLengthInBits % 8 != 0 ){
            columnReadStatus.pageReadStatus.extraBits = (byte) (bytes[bytes.length - 1] << readLengthInBits % 8);
            columnReadStatus.pageReadStatus.bitShift = 8 - (int) readLengthInBits % 8;
          }
          else{
            columnReadStatus.pageReadStatus.extraBits = 0;
            columnReadStatus.pageReadStatus.bitShift = 0;
          }
        }
        else{ // standard read, using memory mapping

          // check if the values in this page did not end on a byte boundary, keep them in temporary storage so they can be added to
          // the beginning of the next page
          if (readLengthInBits % 8 != 0){
            columnReadStatus.pageReadStatus.extraBits = bytes[(int)readLength - 1];
            columnReadStatus.pageReadStatus.bitShift = (int) readLengthInBits % 8;
          }
          else{
            columnReadStatus.pageReadStatus.extraBits = 0;
            columnReadStatus.pageReadStatus.bitShift = 0;
          }
          columnReadStatus.valueVecHolder.getValueVector().data.writeBytes(bytes, (int) readStartInBits,  (int) readLength);
        }

        //columnReadStatus.valueVecHolder.getValueVector().recordCount += currRecordsRead;
        columnReadStatus.valuesReadInCurrentPass += currRecordsRead;
        columnReadStatus.totalValuesRead += currRecordsRead;
        columnReadStatus.pageReadStatus.valuesRead += currRecordsRead;
        if (readStartInBits + readLength >= currBytes.size()) {
          columnReadStatus.pageReadStatus.next();
        } else {
          columnReadStatus.pageReadStatus.readPosInBits = readStartInBits + readLength;
        }
      } while (columnReadStatus.totalValuesRead < recordsToRead && columnReadStatus.pageReadStatus.currentPage != null);
      columnReadStatus.valueVecHolder.getValueVector().recordCount = columnReadStatus.valuesReadInCurrentPass;
    }
  }

  public void readFields() {

  }

  @Override
  public int next() {
    resetBatch();
    long recordsToRead = 0;
    try {
      ColumnReadStatus status =  columnStatuses.values().iterator().next();
      if (allFieldsFixedLength) {
        recordsToRead = Math.min(recordsPerBatch, status.columnChunkMetaData.getValueCount() - status.totalValuesRead);
      } else {
        // set the number of records to read so that reaching a defined maximum will not terminate the read loop
        // the size of the variable length records will determine how many will fit
        recordsToRead = Integer.MAX_VALUE;

        // going to incorporate looking at length of values and copying the data into a single loop, hopefully it won't
        // get too complicated

        //loop through variable length data to find the maximum records that will fit in this batch
        // this will be a bit annoying if we want to loop though row groups, columns, pages and then individual variable
        // length values...
        // jacques believes that variable length fields will be encoded as |length|value|length|value|...
        // cannot find more information on this right now, will keep looking
      }

      if (currentRowGroup == null || status.totalValuesRead == status.columnChunkMetaData.getValueCount()) {
        currentRowGroup = parquetReader.readNextRowGroup();
        if (currentRowGroup == null){
          return 0;
        }
        currentRowGroupIndex++;
      }

      // at the start of a read, and at the beginning of this loop, every column will have read in the same number of values
      while (currentRowGroup != null && status.valuesReadInCurrentPass < recordsToRead) {

        if (allFieldsFixedLength) {
          readAllFixedFields(recordsToRead);
        } else { // variable length columns

        }

        if (columnStatuses.values().iterator().next().pageReadStatus.currentPage == null){
          break;
        }
      }

//      int values = columnStatuses.values().iterator().next().totalValuesRead;
//      for (ColumnReadStatus columnReadStatus : columnStatuses.values()) {
//        for(int i = 0; i < values; i++){
//          System.out.print(columnReadStatus.valueVecHolder.getValueVector().getObject(i) + "," + (i % 25 == 0 ? "\n" + i + " - ": ""));
//        }
//      }

      return columnStatuses.values().iterator().next().totalValuesRead;
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

  static SchemaDefProtos.MajorType toMajorType(PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                               SchemaDefProtos.DataMode mode) {
    return toMajorType(primitiveTypeName, 0, mode);
  }

  static SchemaDefProtos.MajorType toMajorType(PrimitiveType.PrimitiveTypeName primitiveTypeName, int length,
                                               SchemaDefProtos.DataMode mode) {
    switch (primitiveTypeName) {
      case BINARY:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.VARBINARY4).setMode(mode).build();
      case INT64:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.BIGINT).setMode(mode).build();
      case INT32:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.INT).setMode(mode).build();
      case BOOLEAN:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.BOOLEAN).setMode(mode).build();
      case FLOAT:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.FLOAT4).setMode(mode).build();
      case DOUBLE:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.FLOAT8).setMode(mode).build();
      // Both of these are not supported by the parquet library yet (7/3/13),
      // but they are declared here for when they are implemented
      case INT96:
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.FIXEDBINARY).setWidth(12)
            .setMode(mode).build();
      case FIXED_LEN_BYTE_ARRAY:
        checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
        return SchemaDefProtos.MajorType.newBuilder().setMinorType(SchemaDefProtos.MinorType.FIXEDBINARY)
            .setWidth(length).setMode(mode).build();
      default:
        throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
    }
  }

  static String join(String delimiter, String... str) {
    StringBuilder builder = new StringBuilder();
    int i = 0;
    for (String s : str) {
      builder.append(s);
      if (i < str.length) {
        builder.append(delimiter);
      }
      i++;
    }
    return builder.toString();
  }

  @Override
  public void cleanup() {
  }
}
