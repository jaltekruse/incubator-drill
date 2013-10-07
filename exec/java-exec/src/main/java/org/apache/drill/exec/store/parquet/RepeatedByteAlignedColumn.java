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

import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;

public class RepeatedByteAlignedColumn extends UnknownLengthColumn {

  byte[] tempBytes;
  VarBinaryVector tempCurrVec;
  int byteLengthCurrentData;

  RepeatedByteAlignedColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
  }

  @Override
  protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup() throws IOException {
    // write the first 0 offset
    tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
    tempCurrVec.getAccessor().getOffsetVector().getData().writeInt(0);
    bytesReadInCurrentPass = 0;
    valuesReadInCurrentPass = 0;
  }

  @Override
  public int checkNextRecord() throws IOException {

    tempBytes = pageReadStatus.pageDataByteArray;

    byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
        (int) pageReadStatus.readPosInBytes);
    return byteLengthCurrentData;
  }

  @Override
  public void recordLengthCurrentRecord() throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void readRecord() {
    tempBytes = pageReadStatus.pageDataByteArray;
    tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
    tempCurrVec.getAccessor().getOffsetVector().getData().writeInt((int) bytesReadInCurrentPass +
        getCurrentValueLength() - 4 * (int) valuesReadInCurrentPass);
    tempCurrVec.getData().writeBytes(tempBytes, (int) pageReadStatus.readPosInBytes + 4,
        getCurrentValueLength());
    pageReadStatus.readPosInBytes += getCurrentValueLength() + 4;
    bytesReadInCurrentPass += getCurrentValueLength() + 4;
    pageReadStatus.valuesRead++;
    valuesReadInCurrentPass++;
  }

  @Override
  public void afterReading(long recordsReadInCurrentPass) {
    tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
    tempCurrVec.getMutator().setValueCount((int)recordsReadInCurrentPass);
  }

  @Override
  public int getCurrentValueLength() {
    return byteLengthCurrentData;
  }

  @Override
  public int getCountOfRecordsToRead() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int getTotalReadLength() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int beginLoop() {
    //To change body of implemented methods use File | Settings | File Templates.
    return 0;
  }
}
