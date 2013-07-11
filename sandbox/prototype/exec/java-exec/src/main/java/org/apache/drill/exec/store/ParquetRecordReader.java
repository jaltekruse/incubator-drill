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
    int valuesRead;
    // length of single data value in bits
    int dataTypeLengthInBits;

  }

  // class to keep track of the read position of variable length columns
  private class PageReadStatus {
    // store references to the pages that have been uncompressed, but not copied to ValueVectors yet
    Page currentPage;

    PageReader pageReader;
    // read position in the last page in the queue
    int readPos;
    // the number of values read out of the last page
    int valuesRead;

    public boolean next() {
      currentPage = pageReader.readPage();
      if (currentPage == null) {
        return false;
      }
      readPos = 0;
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
      // initialize all of the value vectors, as their sizes are all known
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
      column.valuesRead = 0;
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
  private boolean createColumnStatus(boolean fixedLength, MaterializedField field, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, int allocateSize) throws SchemaChangeException {
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

  public void readAllFixedFields(int recordsToRead) throws IOException {
    for (ColumnReadStatus columnReadStatus : columnStatuses.values()) {
      if (columnReadStatus.pageReadStatus == null) {
        columnReadStatus.pageReadStatus = new PageReadStatus();
      }
      if (columnReadStatus.pageReadStatus.pageReader == null) {
        columnReadStatus.pageReadStatus.pageReader = currentRowGroup.getPageReader(columnReadStatus.columnDescriptor);
      }
      int readStart = 0, readLength = 0;
      do {
        // if no page has been read, or all of the records have been read out of a page, read the next one
        if (columnReadStatus.pageReadStatus.currentPage == null
            || columnReadStatus.pageReadStatus.valuesRead == columnReadStatus.pageReadStatus.currentPage.getValueCount()) {
          columnReadStatus.valuesRead += columnReadStatus.pageReadStatus.valuesRead;
          if (!columnReadStatus.pageReadStatus.next()) {
            break;
          }
        }

        readStart = columnReadStatus.pageReadStatus.readPos;
        currBytes = columnReadStatus.pageReadStatus.currentPage.getBytes();

        // read to the end of the page, or the end of the last value that will fit in the batch
        readLength = Math.min( (columnReadStatus.pageReadStatus.currentPage.getValueCount() * columnReadStatus.dataTypeLengthInBits) / 8,
            ((recordsToRead - columnReadStatus.pageReadStatus.valuesRead) * columnReadStatus.dataTypeLengthInBits) / 8);

        columnReadStatus.valueVecHolder.getValueVector().data.writeBytes(currBytes.toByteArray(), readStart, readStart + readLength);
        int curRecordsRead = ( readLength * 8 ) / columnReadStatus.dataTypeLengthInBits;
        columnReadStatus.valuesRead += curRecordsRead;
        if (readStart + readLength >= currBytes.size()) {
          columnReadStatus.valuesRead += columnReadStatus.pageReadStatus.valuesRead;
          columnReadStatus.pageReadStatus.next();
        } else {
          columnReadStatus.pageReadStatus.valuesRead += curRecordsRead;
          columnReadStatus.pageReadStatus.readPos = readStart + readLength + 1;
        }
      } while (columnReadStatus.valuesRead < recordsToRead && columnReadStatus.pageReadStatus.currentPage != null);
    }
  }

  public void readFields() {

  }

  @Override
  public int next() {
    resetBatch();
    int recordsToRead = 0;
    try {
      if (allFieldsFixedLength) {
        recordsToRead = recordsPerBatch;
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

      if (currentRowGroup == null) {
        currentRowGroup = parquetReader.readNextRowGroup();
        currentRowGroupIndex++;
      }

      /* Pseudo code

      Check if there is a current row group being read, grab the next one if needed
      */

      // at the start of a read, and at the beginning of this loop, every column will have read in the same number of values
      while (currentRowGroup != null && columnStatuses.values().iterator().next().valuesRead < recordsToRead) {

        if (allFieldsFixedLength) {
          readAllFixedFields(recordsToRead);
        } else { // variable length columns

        }

        if (columnStatuses.values().iterator().next().pageReadStatus.currentPage == null){
          break;
        }
      }

      int values = columnStatuses.values().iterator().next().valuesRead;
      for (ColumnReadStatus columnReadStatus : columnStatuses.values()) {
        for(int i = 0; i < values; i++){
          System.out.println(i + " " + columnReadStatus.valueVecHolder.getValueVector().getObject(i));
          if (i == 299){
            Math.min(4,4);
          }
        }
      }

      return columnStatuses.values().iterator().next().valuesRead;
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
