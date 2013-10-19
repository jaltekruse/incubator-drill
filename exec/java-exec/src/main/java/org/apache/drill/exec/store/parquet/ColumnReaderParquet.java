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

import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.store.VectorHolder;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.ValueVector;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.schema.PrimitiveType;

import java.io.IOException;

public abstract class ColumnReaderParquet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnReaderParquet.class);

  // Value Vector for this column
  private VectorHolder valueVecHolder;
  // column description from the parquet library
  private ColumnDescriptor columnDescriptor;
  // metadata of the column, from the parquet library
  private ColumnChunkMetaData columnChunkMetaData;
  // status information on the current page
  private PageReadStatus pageReadStatus;

  private long readPositionInBuffer;

  // quick reference to see if the field is fixed length (as this requires an instanceof)
  private boolean isFixedLength;
  // counter for the total number of values read from one or more pages
  // when a batch is filled all of these values should be the same for each column
  private int totalValuesRead;
  // counter for the values that have been read in this pass (a single call to the next() method)
  private int valuesReadInCurrentPass;
  // length of single data value in bits, if the length is fixed
  private int dataTypeLengthInBits;
  private int bytesReadInCurrentPass;
  private ParquetRecordReader parentReader;

  private ByteBuf vectorData;

  // variables for a single read pass
  private long readStartInBytes = 0;
  private long readLength = 0;
  private long readLengthInBits = 0;
  private long recordsReadInThisIteration = 0;

  ColumnReaderParquet(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
                      boolean fixedLength, ValueVector v){
    this.setParentReader(parentReader);
    if (allocateSize > 1) setValueVecHolder(new VectorHolder(allocateSize, v));
    else setValueVecHolder(new VectorHolder(5000, v));

    setColumnDescriptor(descriptor);
    this.setColumnChunkMetaData(columnChunkMetaData);
    setFixedLength(fixedLength);

    setPageReadStatus(new PageReadStatus(this, parentReader.getRowGroupIndex(), parentReader.getBufferWithAllData()));

    if (parentReader.getRowGroupIndex() != 0) setReadPositionInBuffer(columnChunkMetaData.getFirstDataPageOffset() - 4);
    else setReadPositionInBuffer(columnChunkMetaData.getFirstDataPageOffset());

    if (getColumnDescriptor().getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
      setDataTypeLengthInBits(ParquetRecordReader.getTypeLengthInBits(getColumnDescriptor().getType()));
    }
  }

  public void readAllFixedFields(long recordsToReadInThisPass, ColumnReaderParquet firstColumnStatus) throws IOException {
    setReadStartInBytes(0);
    setReadLength(0);
    setReadLengthInBits(0);
    setRecordsReadInThisIteration(0);
    setVectorData(((BaseValueVector) getValueVecHolder().getValueVector()).getData());
    do {
      // if no page has been read, or all of the records have been read out of a page, read the next one
      if (getPageReadStatus().needNewSubComponent()){
        if (!getPageReadStatus().getNextSubComponent()) {
          break;
        }
      }

      readField( recordsToReadInThisPass, firstColumnStatus);

      setValuesReadInCurrentPass(getValuesReadInCurrentPass() + (int) getRecordsReadInThisIteration());
      setTotalValuesRead(getTotalValuesRead() + (int) getRecordsReadInThisIteration());
    }
    while (getValuesReadInCurrentPass() < recordsToReadInThisPass && getPageReadStatus().hasMoreData());
    getValueVecHolder().getValueVector().getMutator().setValueCount(
        getValuesReadInCurrentPass());
  }

  protected abstract void readField(long recordsToRead, ColumnReaderParquet firstColumnStatus);

  public VectorHolder getValueVecHolder() {
    return valueVecHolder;
  }

  public void setValueVecHolder(VectorHolder valueVecHolder) {
    this.valueVecHolder = valueVecHolder;
  }

  public ColumnDescriptor getColumnDescriptor() {
    return columnDescriptor;
  }

  public void setColumnDescriptor(ColumnDescriptor columnDescriptor) {
    this.columnDescriptor = columnDescriptor;
  }

  public ColumnChunkMetaData getColumnChunkMetaData() {
    return columnChunkMetaData;
  }

  public void setColumnChunkMetaData(ColumnChunkMetaData columnChunkMetaData) {
    this.columnChunkMetaData = columnChunkMetaData;
  }

  public PageReadStatus getPageReadStatus() {
    return pageReadStatus;
  }

  public void setPageReadStatus(PageReadStatus pageReadStatus) {
    this.pageReadStatus = pageReadStatus;
  }

  public long getReadPositionInBuffer() {
    return readPositionInBuffer;
  }

  public void setReadPositionInBuffer(long readPositionInBuffer) {
    this.readPositionInBuffer = readPositionInBuffer;
  }

  public boolean isFixedLength() {
    return isFixedLength;
  }

  public void setFixedLength(boolean fixedLength) {
    isFixedLength = fixedLength;
  }

  public int getTotalValuesRead() {
    return totalValuesRead;
  }

  public void setTotalValuesRead(int totalValuesRead) {
    this.totalValuesRead = totalValuesRead;
  }

  public int getValuesReadInCurrentPass() {
    return valuesReadInCurrentPass;
  }

  public void setValuesReadInCurrentPass(int valuesReadInCurrentPass) {
    this.valuesReadInCurrentPass = valuesReadInCurrentPass;
  }

  public int getDataTypeLengthInBits() {
    return dataTypeLengthInBits;
  }

  public void setDataTypeLengthInBits(int dataTypeLengthInBits) {
    this.dataTypeLengthInBits = dataTypeLengthInBits;
  }

  public int getBytesReadInCurrentPass() {
    return bytesReadInCurrentPass;
  }

  public void setBytesReadInCurrentPass(int bytesReadInCurrentPass) {
    this.bytesReadInCurrentPass = bytesReadInCurrentPass;
  }

  public ParquetRecordReader getParentReader() {
    return parentReader;
  }

  public void setParentReader(ParquetRecordReader parentReader) {
    this.parentReader = parentReader;
  }

  public ByteBuf getVectorData() {
    return vectorData;
  }

  public void setVectorData(ByteBuf vectorData) {
    this.vectorData = vectorData;
  }

  public long getReadStartInBytes() {
    return readStartInBytes;
  }

  public void setReadStartInBytes(long readStartInBytes) {
    this.readStartInBytes = readStartInBytes;
  }

  public long getReadLength() {
    return readLength;
  }

  public void setReadLength(long readLength) {
    this.readLength = readLength;
  }

  public long getReadLengthInBits() {
    return readLengthInBits;
  }

  public void setReadLengthInBits(long readLengthInBits) {
    this.readLengthInBits = readLengthInBits;
  }

  public long getRecordsReadInThisIteration() {
    return recordsReadInThisIteration;
  }

  public void setRecordsReadInThisIteration(long recordsReadInThisIteration) {
    this.recordsReadInThisIteration = recordsReadInThisIteration;
  }
}
