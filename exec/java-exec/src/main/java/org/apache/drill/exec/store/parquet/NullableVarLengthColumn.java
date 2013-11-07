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


import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;

public class NullableVarLengthColumn extends UnknownLengthColumn{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NullableVarLengthColumn.class);

  byte[] tempBytes;
  NullableVarBinaryVector tempCurrNullVec;
  int byteLengthCurrentData;
  int nullsRead;
  boolean currentValNull;
  int totalValuesToRead;
  int defLevelsRead;
  // between the loop to record all of the lengths, and then the loop to read the actual values
  // the read position is used to keep track of position in the parquet file
  // this is used to store its original value to be recalled after reading the lengths and before reading the values
  long initialReadPos;
  int initialValuesRead;

  NullableVarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
  }

  @Override
  protected void readField(long recordsToRead) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup() throws IOException {
    tempCurrNullVec = (NullableVarBinaryVector) getValueVecHolder().getValueVector();
    tempCurrNullVec.getMutator().getVectorWithValues().getAccessor().getOffsetVector().getData().writeInt(0);
    setBytesReadInCurrentPass(0);
    setValuesReadInCurrentPass(0);
    nullsRead = 0;
    totalValuesToRead = 0;
  }


  @Override
  public int checkNextRecord() throws IOException {
    if ( getPageReadStatus().getValuesRead() == getPageReadStatus().getCurrentPage().getValueCount()) {
      return VarLenBinaryReader.PAGE_FINISHED;
    }
    tempBytes = getPageReadStatus().getPageDataByteArray();
    defLevelsRead++;
    if ( getColumnDescriptor().getMaxDefinitionLevel() > getPageReadStatus().getDefinitionLevels().readInteger()){
      currentValNull = true;
      byteLengthCurrentData = 0;
      nullsRead++;
      return 0;// field is null, no length to add to data vector
    }

    byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
        (int) getPageReadStatus().getReadPosInBytes());

    return byteLengthCurrentData;
  }

  @Override
  public void recordLengthCurrentRecord() throws IOException {
    tempCurrNullVec = (NullableVarBinaryVector) getValueVecHolder().getValueVector();
    tempCurrNullVec.getMutator().getVectorWithValues().getAccessor().getOffsetVector().getData()
        .writeInt(
            (int) getBytesReadInCurrentPass() +
                getCurrentValueLength() - 4 * (getValuesReadInCurrentPass() - nullsRead));
    totalValuesToRead++;
    if (getCurrentValueLength() > 0){
      setBytesReadInCurrentPass(getBytesReadInCurrentPass() + getCurrentValueLength() + 4);
      getPageReadStatus().setReadPosInBytes(getPageReadStatus().getReadPosInBytes() + getCurrentValueLength() + 4);
    }

    getPageReadStatus().setValuesRead(getPageReadStatus().getValuesRead() + 1);
    setValuesReadInCurrentPass(getValuesReadInCurrentPass() + 1);
  }

  @Override
  public void readRecord() throws IOException {
    long valuesShouldBeReading = getValuesReadInCurrentPass();
    setValuesReadInCurrentPass(initialValuesRead);
    tempBytes = getPageReadStatus().getPageDataByteArray();
    tempCurrNullVec = (NullableVarBinaryVector) getValueVecHolder().getValueVector();
    getPageReadStatus().setReadPosInBytes(initialReadPos);
    UInt4Vector.Accessor accessor = tempCurrNullVec.getDataVector().getAccessor().getOffsetVector().getAccessor();

    for (int i = 0; i < totalValuesToRead; i++) {
      if (accessor.get(getValuesReadInCurrentPass() + 1) - accessor.get(getValuesReadInCurrentPass()) > 0){
        getPageReadStatus().setReadPosInBytes(getPageReadStatus().getReadPosInBytes() + 4);
        tempCurrNullVec.getData().writeBytes(tempBytes, (int) getPageReadStatus().getReadPosInBytes(),
            accessor.get(getValuesReadInCurrentPass() + 1) - accessor.get(getValuesReadInCurrentPass()));
        tempCurrNullVec.getMutator().setIndexDefined(getValuesReadInCurrentPass());
        getPageReadStatus().setReadPosInBytes(getPageReadStatus().getReadPosInBytes() + accessor.get(getValuesReadInCurrentPass() + 1) - accessor.get(getValuesReadInCurrentPass()));
      }
      setValuesReadInCurrentPass(getValuesReadInCurrentPass() + 1);
    }
  }

  @Override
  public void afterReading(long recordsReadInCurrentPass) {
    tempCurrNullVec = (NullableVarBinaryVector) getValueVecHolder().getValueVector();
    tempCurrNullVec.getMutator().setValueCount((int) recordsReadInCurrentPass);
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
    // TODO - see comment above in non-nullable varlength reader
    throw new UnsupportedOperationException("Current implementation of variable length columns prevents " +
        "reading multiple values at a time, instead values must be read in a loop.");
  }

  @Override
  public int beginLoop() throws IOException {
    totalValuesToRead = 0;
    if (getPageReadStatus().getCurrentPage() == null
        || getPageReadStatus().getValuesRead() == getPageReadStatus().getCurrentPage().getValueCount()) {
      setTotalValuesRead(getTotalValuesRead() + getPageReadStatus().getValuesRead());
      if (!getPageReadStatus().next()) {
        return -1;
      }
      defLevelsRead = 0;
    }
    initialReadPos = getPageReadStatus().getReadPosInBytes();
    initialValuesRead = getValuesReadInCurrentPass();
    return 0;
  }
}
