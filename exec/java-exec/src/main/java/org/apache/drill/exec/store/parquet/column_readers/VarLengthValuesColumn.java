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
package org.apache.drill.exec.store.parquet.column_readers;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.format.Encoding;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.io.api.Binary;

import java.io.IOException;

public abstract class VarLengthValuesColumn<V extends ValueVector> extends VarLengthColumn {

  Binary currDictVal;
  VariableWidthVector variableWidthVector;

  VarLengthValuesColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                        ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                        SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    variableWidthVector = (VariableWidthVector) valueVec;
    if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
      usingDictionary = true;
    }
    else {
      usingDictionary = false;
    }
  }

  @Override
  protected void readField(long recordToRead, ColumnReader firstColumnStatus) {
    dataTypeLengthInBits = variableWidthVector.getAccessor().getValueLength(valuesReadInCurrentPass);
    // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
    boolean success = setSafe((int) valuesReadInCurrentPass, pageReadStatus.pageDataByteArray,
        (int) pageReadStatus.readPosInBytes + 4, dataTypeLengthInBits);
    assert success;
    updatePosition();
  }

  public void updateReadyToReadPosition() {
    pageReadStatus.readyToReadPosInBytes += dataTypeLengthInBits + 4;
    pageReadStatus.valuesReadyToRead++;
    currDictVal = null;
  }

  public void updatePosition() {
    pageReadStatus.readPosInBytes += dataTypeLengthInBits + 4;
    bytesReadInCurrentPass += dataTypeLengthInBits;
    valuesReadInCurrentPass++;
  }

  public boolean skipReadyToReadPositionUpdate() {
    return false;
  }

  protected boolean readAndStoreValueSizeInformation() throws IOException {
    // re-purposing this field here for length in BYTES to prevent repetitive multiplication/division
    dataTypeLengthInBits = BytesUtils.readIntLittleEndian(pageReadStatus.pageDataByteArray,
        (int) pageReadStatus.readyToReadPosInBytes);

    // this should not fail
    if (!variableWidthVector.getMutator().setValueLengthSafe((int) valuesReadInCurrentPass + pageReadStatus.valuesReadyToRead,
        dataTypeLengthInBits)) {
      return true;
    }
    return false;
  }

}
