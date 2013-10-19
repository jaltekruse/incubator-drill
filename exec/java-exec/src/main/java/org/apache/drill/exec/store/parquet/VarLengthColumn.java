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

import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;

// These functions are designed specifically for the variable length type available in parquet today. Unfortunately
// it features length, value, length, value encoding of data that makes it exceedingly difficult to
public class VarLengthColumn extends UnknownLengthColumn {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VarLengthColumn.class);

  byte[] tempBytes;
  VarBinaryVector tempCurrVec;
  int byteLengthCurrentData;
  int totalValuesToRead;
  // between the loop to record all of the lengths, and then the loop to read the actual values
  // the read position is used to keep track of position in the parquet file
  // this is used to store its original value to be recalled after reading the lengths and before reading the values
  long initialReadPos;
  int initialValuesRead;

  VarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
  }

  @Override
  protected void readField(long recordsToRead, ColumnReaderParquet firstColumnStatus) {
    throw new UnsupportedOperationException();
  }


  @Override
  public void setup() {
    // write the first 0 offset
    tempCurrVec = (VarBinaryVector) getValueVecHolder().getValueVector();
    tempCurrVec.getAccessor().getOffsetVector().getData().writeInt(0);
    setBytesReadInCurrentPass(0);
    setValuesReadInCurrentPass(0);
    totalValuesToRead = 0;
  }

  @Override
  public int checkNextRecord() throws IOException {
    if ( getPageReadStatus().getValuesRead() == getPageReadStatus().getCurrentPage().getValueCount()) {
      return VarLenBinaryReader.PAGE_FINISHED;
    }
    tempBytes = getPageReadStatus().getPageDataByteArray();

    byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
        (int) getPageReadStatus().getReadPosInBytes());
    return byteLengthCurrentData;
  }

  @Override
  public void recordLengthCurrentRecord() throws IOException {
    tempCurrVec = (VarBinaryVector) getValueVecHolder().getValueVector();
    int length =  getBytesReadInCurrentPass() +
        getCurrentValueLength() - 4 * getValuesReadInCurrentPass();
    tempCurrVec.getAccessor().getOffsetVector().getData().writeInt(length);
    totalValuesToRead++;
    setBytesReadInCurrentPass(getBytesReadInCurrentPass() + getCurrentValueLength() + 4);
    getPageReadStatus().setReadPosInBytes(getPageReadStatus().getReadPosInBytes() + getCurrentValueLength() + 4);
    getPageReadStatus().setValuesRead(getPageReadStatus().getValuesRead() + 1);
    setValuesReadInCurrentPass(getValuesReadInCurrentPass() + 1);
  }

  @Override
  public void readRecord() {
    setValuesReadInCurrentPass(initialValuesRead);
    getPageReadStatus().setReadPosInBytes(initialReadPos);
    tempBytes = getPageReadStatus().getPageDataByteArray();
    UInt4Vector.Accessor accessor = tempCurrVec.getAccessor().getOffsetVector().getAccessor();
    tempCurrVec = (VarBinaryVector) getValueVecHolder().getValueVector();
    for (int i = 0; i < totalValuesToRead; i++) {
      getPageReadStatus().setReadPosInBytes(getPageReadStatus().getReadPosInBytes() + 4);

      tempCurrVec.getData().writeBytes(tempBytes, (int) getPageReadStatus().getReadPosInBytes(),
          accessor.get(getValuesReadInCurrentPass() + 1) - accessor.get(getValuesReadInCurrentPass()));
      getPageReadStatus().setReadPosInBytes(getPageReadStatus().getReadPosInBytes() + accessor.get(getValuesReadInCurrentPass() + 1) - accessor.get(getValuesReadInCurrentPass()));
      setValuesReadInCurrentPass(getValuesReadInCurrentPass() + 1);
    }
  }

  @Override
  public void afterReading(long recordsReadInCurrentPass) {
    tempCurrVec = (VarBinaryVector) getValueVecHolder().getValueVector();
    tempCurrVec.getMutator().setValueCount((int) recordsReadInCurrentPass);
  }

  @Override
  public int getCurrentValueLength() {
    return byteLengthCurrentData;
  }

  @Override
  public int getCountOfRecordsToRead() {
    return totalValuesToRead;
  }

  @Override
  public int getTotalReadLength() {
    // TODO - this is supposed to be addressed soon by the Parquet team at twitter, unfortunately because the
    // length/value scheme was in the format, we will likely have to support it for quite some time, but
    // hopefully the new format with the data and lengths separated will become default when it is finished
    throw new UnsupportedOperationException("Current implementation of variable length columns prevents " +
        "reading multiple values at a time.");
  }

  @Override
  public int beginLoop() throws IOException {
    totalValuesToRead = 0;
    if (getPageReadStatus().getCurrentPage() == null
        || getPageReadStatus().getValuesRead() == getPageReadStatus().getCurrentPage().getValueCount()) {
      if ( getPageReadStatus().getCurrentPage() != null && getPageReadStatus().getValuesRead() != getPageReadStatus().getCurrentPage().getValueCount())
        logger.error("incorrect number of records read from variable length column");
      setTotalValuesRead(getTotalValuesRead() + getPageReadStatus().getValuesRead());
      if (!getPageReadStatus().next()) {
        return VarLenBinaryReader.ROW_GROUP_FINISHED;
      }
    }
    initialReadPos = getPageReadStatus().getReadPosInBytes();
    initialValuesRead = getValuesReadInCurrentPass();
    return 0;
  }
}
