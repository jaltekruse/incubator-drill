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
package org.apache.drill.exec.store.parquet.columnreaders;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableVectorDefinitionSetter;
import org.apache.drill.exec.vector.ValueVector;
import parquet.column.ColumnDescriptor;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;

abstract class NullableColumnReader<V extends ValueVector> extends ColumnReader<V>{

  int nullsFound;
  // used to skip nulls found
  int rightBitShift;
  // used when copying less than a byte worth of data at a time, to indicate the number of used bits in the current byte
  int bitsUsed;
  BaseValueVector castedBaseVector;
  NullableVectorDefinitionSetter castedVectorMutator;

  NullableColumnReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
               boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
    castedBaseVector = (BaseValueVector) v;
    castedVectorMutator = (NullableVectorDefinitionSetter) v.getMutator();
  }


  public void processPages(long recordsToReadInThisPass) throws IOException {
    int totalDefinitionLevelsRead = 0;
    readStartInBytes = 0;
    readLength = 0;
    readLengthInBits = 0;
    recordsReadInThisIteration = 0;
    vectorData = castedBaseVector.getData();
    int currentValueIndexInVector;

    do {
      // if no page has been read, or all of the records have been read out of a page, read the next one
      if (pageReader.currentPage == null
          || pageReader.valuesRead == pageReader.currentPage.getValueCount()) {
        if (!pageReader.next()) {
          break;
        }
      }

      // values need to be spaced out where nulls appear in the column
      // leaving blank space for nulls allows for random access to values
      // to optimize copying data out of the buffered disk stream, runs of defined values
      // are located and copied together, rather than copying individual values

      long runStart = pageReader.readPosInBytes;
      int runLength;
      int currentDefinitionLevel;
      currentValueIndexInVector = (int) recordsReadInThisIteration;
      boolean lastValueWasNull;
      int definitionLevelsRead = 0;
      boolean lastRunBrokenByNull = false;
      // loop to find the longest run of defined values available, can be preceded by several nulls
      while (true){
        lastValueWasNull = true;
        //  seemed to be getting the off by one error at page boundary
        // tried fixing it with this, seemed to have gotten rid of the shift, but
        // messed up the e
        if (lastRunBrokenByNull && pageReader.valuesRead > 0) {
//        if (lastRunBrokenByNull ) {
          nullsFound = 1;
          currentValueIndexInVector--;
          lastRunBrokenByNull = false;
        } else  {
          nullsFound = 0;
        }
        runLength = 0;
        if (totalDefinitionLevelsRead == recordsToReadInThisPass
            || totalDefinitionLevelsRead >= valueVec.getValueCapacity()) {
          break;
        }
        while(totalDefinitionLevelsRead < recordsToReadInThisPass
            && totalDefinitionLevelsRead < valueVec.getValueCapacity()
            && definitionLevelsRead < pageReader.currentPage.getValueCount()){
          currentDefinitionLevel = pageReader.definitionLevels.readInteger();
          definitionLevelsRead++;
          totalDefinitionLevelsRead++;
          if ( currentDefinitionLevel < columnDescriptor.getMaxDefinitionLevel()){
            // a run of non-null values was found, break out of this loop to do a read in the outer loop
            if ( ! lastValueWasNull ){
              currentValueIndexInVector++;
              lastRunBrokenByNull = true;
              break;
            }
            nullsFound++;
            lastValueWasNull = true;
          }
          else{
            if (lastValueWasNull){
              runLength = 0;
              lastValueWasNull = false;
            }
            runLength++;
            castedVectorMutator.setIndexDefined(totalDefinitionLevelsRead - 1);
          }
          currentValueIndexInVector++;
        }
        valuesReadInCurrentPass += nullsFound;

        int writerIndex = ((BaseValueVector) valueVec).getData().writerIndex();
        if ( dataTypeLengthInBits > 8  || (dataTypeLengthInBits < 8 && totalValuesRead + runLength % 8 == 0)){
          castedBaseVector.getData().setIndex(0, writerIndex + (int) Math.ceil( nullsFound * dataTypeLengthInBits / 8.0));
        }
        else if (dataTypeLengthInBits < 8){
          rightBitShift += dataTypeLengthInBits * nullsFound;
        }
        this.recordsReadInThisIteration = runLength;

        // set up metadata
        this.readStartInBytes = pageReader.readPosInBytes;
        this.readLengthInBits = recordsReadInThisIteration * dataTypeLengthInBits;
        this.readLength = (int) Math.ceil(readLengthInBits / 8.0);
        readField( runLength);
        recordsReadInThisIteration += nullsFound;
        valuesReadInCurrentPass += runLength;
        totalValuesRead += recordsReadInThisIteration;
        pageReader.valuesRead += recordsReadInThisIteration;
        if ( (readStartInBytes + readLength >= pageReader.byteLength && bitsUsed == 0) &&
            pageReader.valuesRead >= pageReader.currentPage.getValueCount()) {
          if (!pageReader.next()) {
            break;
          }
          definitionLevelsRead = 0;
        } else {
          pageReader.readPosInBytes = readStartInBytes + readLength;
        }
      }
    } while (totalDefinitionLevelsRead < recordsToReadInThisPass && pageReader.currentPage != null);
    valueVec.getMutator().setValueCount(
        valuesReadInCurrentPass);
  }

  protected abstract void readField(long recordsToRead);
}
