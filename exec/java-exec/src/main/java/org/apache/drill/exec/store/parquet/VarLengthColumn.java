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
  protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
    throw new UnsupportedOperationException();
  }


  @Override
  public void setup() {
    // write the first 0 offset
    tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
    tempCurrVec.getAccessor().getOffsetVector().getData().writeInt(0);
    bytesReadInCurrentPass = 0;
    valuesReadInCurrentPass = 0;
    totalValuesToRead = 0;
  }

  @Override
  public int checkNextRecord() throws IOException {
    if (pageReadStatus.currentPage == null
        || pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
      totalValuesRead += pageReadStatus.valuesRead;
      if (!pageReadStatus.next()) {
        return VarLenBinaryReader.ROW_GROUP_FINISHED;
      }
    }
    tempBytes = pageReadStatus.pageDataByteArray;

    try{
    byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
        (int) pageReadStatus.readPosInBytes);
    }
    catch(Exception ex){
      Math.min(3, 4);
    }
    return byteLengthCurrentData;
  }

  @Override
  public void recordLengthCurrentRecord() throws IOException {
    tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
    int length =  bytesReadInCurrentPass +
        getCurrentValueLength() - 4 * valuesReadInCurrentPass;
    tempCurrVec.getAccessor().getOffsetVector().getData().writeInt(length);
    totalValuesToRead++;
    bytesReadInCurrentPass += getCurrentValueLength() + 4;
    pageReadStatus.readPosInBytes += getCurrentValueLength() + 4;
    pageReadStatus.valuesRead++;
    valuesReadInCurrentPass++;
  }

  @Override
  public void readRecord() {
    valuesReadInCurrentPass = initialValuesRead;
    pageReadStatus.readPosInBytes = initialReadPos;
    tempBytes = pageReadStatus.pageDataByteArray;
    UInt4Vector.Accessor accessor = tempCurrVec.getAccessor().getOffsetVector().getAccessor();
    tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
    for (int i = 0; i < totalValuesToRead; i++) {
      pageReadStatus.readPosInBytes += 4;
      tempCurrVec.getData().writeBytes(tempBytes, (int) pageReadStatus.readPosInBytes,
          accessor.get(valuesReadInCurrentPass + 1) - accessor.get(valuesReadInCurrentPass));
      pageReadStatus.readPosInBytes += accessor.get(valuesReadInCurrentPass + 1) - accessor.get(valuesReadInCurrentPass);
      valuesReadInCurrentPass++;
    }
  }

  @Override
  public void afterReading(long recordsReadInCurrentPass) {
    tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
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
    initialReadPos = pageReadStatus.readPosInBytes;
    initialValuesRead = valuesReadInCurrentPass;
    //totalValuesToRead = 0;
    if (pageReadStatus.currentPage == null
        || pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
      totalValuesRead += pageReadStatus.valuesRead;
      if (!pageReadStatus.next()) {
        return VarLenBinaryReader.ROW_GROUP_FINISHED;
      }
    }
    return 0;
  }
}
