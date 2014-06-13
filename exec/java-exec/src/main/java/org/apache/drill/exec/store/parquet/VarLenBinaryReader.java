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

import org.apache.drill.exec.store.parquet.VarLengthColumnReaders.*;
import parquet.bytes.BytesUtils;

import java.io.IOException;
import java.util.List;

public class VarLenBinaryReader {

  ParquetRecordReader parentReader;
  final List<VarLengthColumn> columns;

  public VarLenBinaryReader(ParquetRecordReader parentReader, List<VarLengthColumn> columns){
    this.parentReader = parentReader;
    this.columns = columns;
  }

  /**
   * Reads as many variable length values as possible.
   *
   * @param recordsToReadInThisPass - the number of records recommended for reading form the reader
   * @param firstColumnStatus - a reference to the first column status in the parquet file to grab metatdata from
   * @return - the number of fixed length fields that will fit in the batch
   * @throws IOException
   */
  public long readFields(long recordsToReadInThisPass, ColumnReader firstColumnStatus) throws IOException {

    long recordsReadInCurrentPass = 0;
    int lengthVarFieldsInCurrentRecord;
    boolean rowGroupFinished = false;
    byte[] bytes;
    // write the first 0 offset
    for (VarLengthColumn columnReader : columns) {
      columnReader.reset();
    }

    outer: do {
      lengthVarFieldsInCurrentRecord = 0;
      for (VarLengthColumn columnReader : columns) {
        rowGroupFinished = columnReader.determineSize(recordsReadInCurrentPass, lengthVarFieldsInCurrentRecord);
      }
      // check that the next record will fit in the batch
      if (rowGroupFinished || (recordsReadInCurrentPass + 1) * parentReader.getBitWidthAllFixedFields() + lengthVarFieldsInCurrentRecord
          > parentReader.getBatchSize()){
        break outer;
      }
      recordsReadInCurrentPass++;
    } while (recordsReadInCurrentPass < recordsToReadInThisPass);

    for (VarLengthColumn columnReader : columns) {
      // TODO - break up the updatePosition method into one for each of the loops (determine read length and actually reading)
      // the way this works it is incrementing this counter twice
      //columnReader.valuesReadInCurrentPass = 0;
    }
    for (VarLengthColumn columnReader : columns) {
      columnReader.readRecords(columnReader.pageReadStatus.valuesReadyToRead);
    }
    for (VarLengthColumn columnReader : columns) {
      columnReader.valueVec.getMutator().setValueCount((int) recordsReadInCurrentPass);
    }
    return recordsReadInCurrentPass;
  }
}