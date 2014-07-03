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

import org.apache.drill.exec.vector.RepeatedFixedWidthVector;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.vector.ValueVector;
import parquet.column.ColumnDescriptor;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;

public class FixedWidthRepeatedReader extends VarLengthColumn {

  RepeatedFixedWidthVector castedRepeatedVector;
  ColumnReader dataReader;
  int dataTypeLengthInBytes;
  // we can do a vector copy of the data once we figure out how much we need to copy
  // this tracks the number of values to transfer (the dataReader will translate this to a number
  // of bytes to transfer and re-use the code from the non-repeated types)
  int valuesToRead;
  int repeatedGroupsReadInCurrentPass;
  // stores the number of
  int repeatedValuesInCurrentList;
  // empty lists are notated by definition levels, to stop reading at the correct time, we must keep
  // track of the number of empty lists as well as the length of all of the defined lists together
  int definitionLevelsRead;
  // parquet currently does not restrict lists reaching across pages for repeated values, this necessitates
  // tracking when this happens to stop some of the state updates until we know the full length of the repeated
  // value for the current record
  boolean notFishedReadingList;

  FixedWidthRepeatedReader(ParquetRecordReader parentReader, ColumnReader dataReader, int dataTypeLengthInBytes, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector valueVector, SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, valueVector, schemaElement);
    castedRepeatedVector = (RepeatedFixedWidthVector) valueVector;
    this.dataTypeLengthInBytes = dataTypeLengthInBytes;
    this.dataReader = dataReader;
    this.dataReader.pageReadStatus = this.pageReadStatus;
  }

  public void reset() {
    bytesReadInCurrentPass = 0;
    valuesReadInCurrentPass = 0;
    pageReadStatus.valuesReadyToRead = 0;
    dataReader.vectorData = castedRepeatedVector.getMutator().getDataVector().getData();
    dataReader.valuesReadInCurrentPass = 0;
    repeatedGroupsReadInCurrentPass = 0;
    notFishedReadingList = false;
  }

  public int getRecordsReadInCurrentPass() {
    return repeatedGroupsReadInCurrentPass;
  }

  @Override
  protected void readField(long recordsToRead) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public boolean skipReadyToReadPositionUpdate() {
    return false;
  }

  public void updateReadyToReadPosition() {
    valuesToRead += repeatedValuesInCurrentList;
    pageReadStatus.valuesReadyToRead += repeatedValuesInCurrentList;
    repeatedGroupsReadInCurrentPass++;
    currDictVal = null;
    repeatedValuesInCurrentList = -1;
  }

  public void updatePosition() {
    pageReadStatus.readPosInBytes += dataTypeLengthInBits;
    bytesReadInCurrentPass += dataTypeLengthInBits;
    valuesReadInCurrentPass++;
  }

  public void hitRowGroupEnd() {
    pageReadStatus.valuesReadyToRead = 0;
    definitionLevelsRead = 0;
  }

  public void postPageRead() {
    super.postPageRead();
    repeatedValuesInCurrentList = -1;
    definitionLevelsRead = 0;
  }

  protected int totalValuesReadAndReadyToReadInPage() {
    return definitionLevelsRead;
  }

  protected boolean checkVectorCapacityReached() {
    boolean doneReading = super.checkVectorCapacityReached();
    if (doneReading)
      return true;
    if (valuesReadInCurrentPass + pageReadStatus.valuesReadyToRead + repeatedValuesInCurrentList >= valueVec.getValueCapacity())
      return true;
    else
      return false;
  }

  protected boolean readAndStoreValueSizeInformation() {
    if ( currDefLevel == -1 ) {
      currDefLevel = pageReadStatus.definitionLevels.readInteger();
      definitionLevelsRead++;
    }
    int repLevel;
    if ( columnDescriptor.getMaxDefinitionLevel() == currDefLevel){
      if (repeatedValuesInCurrentList == -1) {
        repeatedValuesInCurrentList = 1;
        do {
          repLevel = pageReadStatus.repetitionLevels.readInteger();
          if (repLevel > 0) {
            repeatedValuesInCurrentList++;
            currDefLevel = pageReadStatus.definitionLevels.readInteger();
            definitionLevelsRead++;
            // TODO - may need to add a condition here for the end of the row group, as this is currently causing
            // us to skip the writing of the value length into the vector, and bring us back to reading the next page
            // a missing next page will currently cause this method to exit early.
            // we hit the end of this page, without confirmation that we reached the end of the current record
            if (definitionLevelsRead == pageReadStatus.currentPage.getValueCount()) {
              // check that we have not hit the end of the row group (in which case we will not find the repetition level indicating
              // the end of this record as there is no next page to check, we have read all the values in this repetition so it is okay
              // to add it to the read )
              if (totalValuesRead + pageReadStatus.valuesReadyToRead + repeatedValuesInCurrentList != columnChunkMetaData.getValueCount()){
                notFishedReadingList = true;
                //return false;
              }
            }
          }
        } while (repLevel != 0);
      }
    }
    else {
      repeatedValuesInCurrentList = 0;
    }
    // this should not fail
    if (!castedRepeatedVector.getMutator().setRepetitionAtIndexSafe(repeatedGroupsReadInCurrentPass,
        repeatedValuesInCurrentList)) {
      return true;
    }
    // This field is being referenced in the superclass determineSize method, so we need to set it here
    // again going to make this the length in BYTES to avoid repetitive multiplication/division
    dataTypeLengthInBits = repeatedValuesInCurrentList * dataTypeLengthInBytes;
    return false;
  }

  protected void readRecords(int valuesToRead) {
    if (valuesToRead == 0) return;
    dataReader.readValues(valuesToRead);
    //pageReadStatus.valuesRead += definitionLevelsRead;
    valuesReadInCurrentPass += valuesToRead;
    try {
      castedRepeatedVector.getMutator().setValueCounts(repeatedGroupsReadInCurrentPass, valuesReadInCurrentPass);
    } catch (Throwable t) {
      throw t;
    }
  }

  @Override
  public int capacity() {
    return castedRepeatedVector.getMutator().getDataVector().getData().capacity();
  }
}

