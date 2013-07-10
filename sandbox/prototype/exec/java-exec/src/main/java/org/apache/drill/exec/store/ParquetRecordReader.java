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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
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

import javax.management.AttributeList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
    VectorHolder valueVec;
    // column description from the parquet library
    ColumnDescriptor parquetColumnDescriptor;
    // metadata of the column, from the parquet library
    ColumnChunkMetaData columnChunkMetaData;
    // status information on the current page
    PageReadStatus pageReadStatus;
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
  }

  // this class represents a row group, it is named poorly in the parquet library
  private PageReadStore currentRowGroup;
  private HashMap<MaterializedField, ColumnReadStatus> columns;


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
  public int getTypeLengthInBytes(PrimitiveType.PrimitiveTypeName type) {
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
    columns = new HashMap();
    currentRowGroup = null;

    List<ColumnDescriptor> columns = schema.getColumns();
    allFieldsFixedLength = true;
    ColumnDescriptor column;
    ColumnChunkMetaData columnChunkMetaData;
    SchemaBuilder builder = BatchSchema.newBuilder();
    for (int i = 0; i < columns.size(); ++i) {
      column = columns.get(i);
      columnChunkMetaData = footer.getBlocks().get(i).getColumns().get(i);

      // sum the lengths of all of the fixed length fields
      if (column.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
        // There is not support for the fixed binary type yet in parquet, leaving a task here as a reminder
        // TODO - implement this when the feature is added upstream
//          if (column.getType() != PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY){
//              byteWidthAllFixedFields += column.getType().getWidth()
//          }
        bitWidthAllFixedFields += getTypeLengthInBytes(column.getType());
      } else {
        allFieldsFixedLength = false;
      }
      MaterializedField field = MaterializedField.create(new SchemaPath(toFieldName(column.getPath())),
          toMajorType(column.getType(), getDataMode(column)));

      builder.addField(field);
    }
    currentSchema = builder.build();

    if (allFieldsFixedLength) {
      try {
        recordsPerBatch = DEFAULT_LENGTH_IN_BITS / bitWidthAllFixedFields;
        for (MaterializedField field : currentSchema) {
          getOrCreateVectorHolder(field, TypeHelper.getSize(field.getType()));
        }
      } catch (SchemaChangeException e) {
        throw new DrillRuntimeException(e);
      }
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
    for (ColumnReadStatus column : columns.values()) {
      column.valueVec.reset();
    }
  }

  // might want to update this to create an entire column read status and add it to the columns map
  private boolean createColumnStatus(MaterializedField field, ColumnDescriptor descriptor, int allocateSize) throws SchemaChangeException {
    SchemaDefProtos.MajorType type = field.getType();
    MaterializedField f = MaterializedField.create(new SchemaPath(field.getName()), type);
    ValueVector.Base v = TypeHelper.getNewVector(f, allocator);
    v.allocateNew(allocateSize);
    ColumnReadStatus newCol = new ColumnReadStatus();
    newCol.valueVec = new VectorHolder(allocateSize, v);
    newCol.parquetColumnDescriptor = descriptor;
    columns.put(field, newCol);
    outputMutator.addField(fieldId, v);
    return true;
  }

  // created this method to remove extra logic in the method for creating a new valuevector
  // as the schema will only change between file or row groups, there is no need to check that a field exists
  // every time we want to access it
  private ColumnReadStatus getColumnStatus(MaterializedField field) {
    return columns.get(field);
  }

  @Override
  public int next() {
    resetBatch();
    int newRecordCount = 0;
    int recordsToRead = 0;
    try {

      if (allFieldsFixedLength) {
        recordsToRead = recordsPerBatch;
      } else {

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

      while (currentRowGroup != null && recordsToRead < recordsToRead) {

        for (ColumnChunkMetaData column : footer.getBlocks().get(currentRowGroupIndex).getColumns()) {

          ColumnDescriptor descriptor = columns.get(field)

          PageReader pageReader = currentRowGroup.getPageReader(descriptor);

          if (pageReader == null) {
            continue;
          }

          Page p = pageReader.readPage();

          VectorHolder holder = valueVectorMap.get(field.getFieldId());

          int recordsRead = newRecordCount;
          if (descriptor.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
            boolean finishedLastPage = previousPageFinished;
            int readStart = 0, readEnd = 0, typeLength = 0;
            while (recordsRead < recordsToRead && p != null) {
              readStart = 0;
              currBytes = p.getBytes();
              typeLength = getTypeLengthInBytes(descriptor.getType());

              if (!finishedLastPage) {
                readStart = typeLength * recordsReadFromPage;
                finishedLastPage = true;
              }

              // read to the end of the page, or the end of the last value that will fit in the batch
              readEnd = Math.min(p.getValueCount() * typeLength, (recordsToRead - newRecordCount) * typeLength);

              holder.getValueVector().data.writeBytes(currBytes.toByteArray(), readStart, readEnd);
              recordsRead += (readEnd - readStart) / typeLength;
              p = pageReader.readPage();
            }

            //FIXME: (Tim) This flag is a global flag but each individual column has its own page.
            // Can we safely assume all column pages have the exact same length of bytes?
            // If that's true than we just need one index and one flag.
            // If not we need tracking for each column.


            // the last page of this row group was read
            if (p == null) {
              previousPageFinished = true;
              // FIXME: (Tim) Not all pages have finished reading their pages? Just one of them in this case right?
            }
            // the end of the page was not reached with the last read, set up for the next read
            else if (readEnd < p.getValueCount() * typeLength) {
              previousPageFinished = false;
              recordsReadFromPage = (readEnd - readStart) / typeLength;
            } else {
              previousPageFinished = true;
            }
          } else { // TODO - variable length columns

          }
        }
      }

      return newRecordCount;
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
