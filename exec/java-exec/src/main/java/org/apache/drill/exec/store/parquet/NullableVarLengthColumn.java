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
package org.apache.drill.exec.store.parquet;


import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;

public class NullableVarLengthColumn extends UnknownLengthColumn{

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
  protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup() throws IOException {
    tempCurrNullVec = (NullableVarBinaryVector) valueVecHolder.getValueVector();
    tempCurrNullVec.getMutator().getVectorWithValues().getAccessor().getOffsetVector().getData().writeInt(0);
    bytesReadInCurrentPass = 0;
    valuesReadInCurrentPass = 0;
    nullsRead = 0;
    totalValuesToRead = 0;
  }


  @Override
  public int checkNextRecord() throws IOException {
    if ( pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
      return VarLenBinaryReader.PAGE_FINISHED;
    }
    tempBytes = pageReadStatus.pageDataByteArray;
    try{
      defLevelsRead++;
      if ( columnDescriptor.getMaxDefinitionLevel() > pageReadStatus.definitionLevels.readInteger()){
        currentValNull = true;
        byteLengthCurrentData = 0;
        nullsRead++;
        return 0;// field is null, no length to add to data vector
      }

      byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
          (int) pageReadStatus.readPosInBytes);
    }
    catch(Exception ex){
      Math.min(4,5);
    }
    return byteLengthCurrentData;
  }

  @Override
  public void recordLengthCurrentRecord() throws IOException {
    tempCurrNullVec = (NullableVarBinaryVector) valueVecHolder.getValueVector();
    tempCurrNullVec.getMutator().getVectorWithValues().getAccessor().getOffsetVector().getData()
        .writeInt(
            (int) bytesReadInCurrentPass  +
                getCurrentValueLength() - 4 * (valuesReadInCurrentPass - nullsRead
                /*(currentValNull ? Math.max (0, nullsRead - 1) : nullsRead )*/));
    totalValuesToRead++;
    if (getCurrentValueLength() > 0){
      bytesReadInCurrentPass += getCurrentValueLength() + 4;
      pageReadStatus.readPosInBytes += getCurrentValueLength() + 4;
    }

    pageReadStatus.valuesRead++;
    valuesReadInCurrentPass++;
  }

  @Override
  public void readRecord() throws IOException {
    long valuesShouldBeReading = valuesReadInCurrentPass;
    valuesReadInCurrentPass = initialValuesRead;
    tempBytes = pageReadStatus.pageDataByteArray;
    tempCurrNullVec = (NullableVarBinaryVector) valueVecHolder.getValueVector();
    pageReadStatus.readPosInBytes = initialReadPos;
    UInt4Vector.Accessor accessor = tempCurrNullVec.getDataVector().getAccessor().getOffsetVector().getAccessor();

    for (int i = 0; i < totalValuesToRead; i++) {
      if (accessor.get(valuesReadInCurrentPass + 1) - accessor.get(valuesReadInCurrentPass) > 0){
        pageReadStatus.readPosInBytes += 4;
        tempCurrNullVec.getData().writeBytes(tempBytes, (int) pageReadStatus.readPosInBytes,
            accessor.get(valuesReadInCurrentPass + 1) - accessor.get(valuesReadInCurrentPass));
        tempCurrNullVec.getMutator().setIndexDefined(valuesReadInCurrentPass);
        pageReadStatus.readPosInBytes += accessor.get(valuesReadInCurrentPass + 1) - accessor.get(valuesReadInCurrentPass);
      }
      valuesReadInCurrentPass++;
    }
//    if (valuesReadInCurrentPass != valuesReadInCurrentPass){
//      Math.min(3,2);
//    }
//
//      // reached the end of a page
//      if ( pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
//        pageReadStatus.next();
//      }
  }

  @Override
  public void afterReading(long recordsReadInCurrentPass) {
    tempCurrNullVec = (NullableVarBinaryVector) valueVecHolder.getValueVector();
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
    if (pageReadStatus.currentPage == null
        || pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
      totalValuesRead += pageReadStatus.valuesRead;
      if (!pageReadStatus.next()) {
        return -1;
      }
      defLevelsRead = 0;
    }
    initialReadPos = pageReadStatus.readPosInBytes;
    initialValuesRead = valuesReadInCurrentPass;
    return 0;
  }
}
