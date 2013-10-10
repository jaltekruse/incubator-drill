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

import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableVectorDefinitionSetter;
import org.apache.drill.exec.vector.ValueVector;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;

public abstract class NullableColumnReader extends ColumnReaderParquet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NullableColumnReader.class);

  int nullsFound;
  // used to skip nulls found
  int rightBitShift;
  // used when copying less than a byte worth of data at a time, to indicate the number of used bits in the current byte
  int bitsUsed;

  NullableColumnReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
               boolean fixedLength, ValueVector v){
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
  }

  public void readAllFixedFields(long recordsToReadInThisPass, ColumnReaderParquet firstColumnStatus) throws IOException {
    setReadStartInBytes(0);
    setReadLength(0);
    setReadLengthInBits(0);
    setRecordsReadInThisIteration(0);
    setVectorData(((BaseValueVector) getValueVecHolder().getValueVector()).getData());

    do {
      // if no page has been read, or all of the records have been read out of a page, read the next one
      if (getPageReadStatus().currentPage == null
          || getPageReadStatus().valuesRead == getPageReadStatus().currentPage.getValueCount()) {
        if (!getPageReadStatus().next()) {
          break;
        }
      }

      // values need to be spaced out where nulls appear in the column
      // leaving blank space for nulls allows for random access to values
      // to optimize copying data out of the buffered disk stream, runs of defined values
      // are located and copied together, rather than copying individual values

      long runStart = getPageReadStatus().readPosInBytes;
      int runLength = 0;
      int currentDefinitionLevel = 0;
      int currentValueIndexInVector = getValuesReadInCurrentPass();
      boolean lastValueWasNull = true;
      int definitionLevelsRead;
        definitionLevelsRead = 0;
        lastValueWasNull = true;
        nullsFound = 0;
        if (currentValueIndexInVector - getTotalValuesRead() == recordsToReadInThisPass
            || currentValueIndexInVector >= getValueVecHolder().getValueVector().getValueCapacity()){
          break;
        }
        // loop to find the longest run of defined values available, can be preceded by several nulls
        while(currentValueIndexInVector - getTotalValuesRead() < recordsToReadInThisPass
            && currentValueIndexInVector < getValueVecHolder().getValueVector().getValueCapacity()
            && getPageReadStatus().valuesRead + definitionLevelsRead < getPageReadStatus().currentPage.getValueCount()){
          currentDefinitionLevel = getPageReadStatus().definitionLevels.readInteger();
          definitionLevelsRead++;
          if ( currentDefinitionLevel < getColumnDescriptor().getMaxDefinitionLevel()){
            // a run of non-null values was found, break out of this loop to do a read in the outer loop
            nullsFound++;
            if ( ! lastValueWasNull ){
              currentValueIndexInVector++;
              break;
            }
            lastValueWasNull = true;
          }
          else{
            if (lastValueWasNull){
              runStart = getPageReadStatus().readPosInBytes;
              runLength = 0;
              lastValueWasNull = false;
            }
            runLength++;
            ((NullableVectorDefinitionSetter) getValueVecHolder().getValueVector().getMutator()).setIndexDefined(currentValueIndexInVector);
          }
          currentValueIndexInVector++;
        }
        getPageReadStatus().readPosInBytes = runStart;
        setRecordsReadInThisIteration(runLength);

        readField( runLength, firstColumnStatus);
        int writerIndex = ((BaseValueVector) getValueVecHolder().getValueVector()).getData().writerIndex();
        if ( getDataTypeLengthInBits() > 8  || (getDataTypeLengthInBits() < 8 && getTotalValuesRead() + runLength % 8 == 0)){
          ((BaseValueVector) getValueVecHolder().getValueVector()).getData().setIndex(0, writerIndex + (int) Math.ceil( nullsFound * getDataTypeLengthInBits() / 8.0));
        }
        else if (getDataTypeLengthInBits() < 8){
          rightBitShift += getDataTypeLengthInBits() * nullsFound;
        }
        setRecordsReadInThisIteration(getRecordsReadInThisIteration() + nullsFound);
        setValuesReadInCurrentPass(getValuesReadInCurrentPass() + (int) getRecordsReadInThisIteration());
        setTotalValuesRead(getTotalValuesRead() + (int) getRecordsReadInThisIteration());
        getPageReadStatus().valuesRead += getRecordsReadInThisIteration();
        if (getReadStartInBytes() + getReadLength() >= getPageReadStatus().byteLength && bitsUsed == 0) {
          getPageReadStatus().next();
        } else {
          getPageReadStatus().readPosInBytes = getReadStartInBytes() + getReadLength();
        }
    }
    while (getValuesReadInCurrentPass() < recordsToReadInThisPass && getPageReadStatus().currentPage != null);
    getValueVecHolder().getValueVector().getMutator().setValueCount(
        getValuesReadInCurrentPass());
  }

  protected abstract void readField(long recordsToRead, ColumnReaderParquet firstColumnStatus);
}