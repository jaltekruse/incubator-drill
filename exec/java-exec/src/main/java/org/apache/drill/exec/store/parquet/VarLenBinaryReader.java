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
import org.apache.drill.exec.vector.VarBinaryVector;
import parquet.bytes.BytesUtils;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;
import java.util.List;

public class VarLenBinaryReader {

  ParquetRecordReader parentReader;
  final List<UnknownLengthColumn> columns;
  long recordsReadInCurrentPass;
  static final int ROW_GROUP_FINISHED = -1;
  static final int PAGE_FINISHED = -2;

  public VarLenBinaryReader(ParquetRecordReader parentReader, List<UnknownLengthColumn> columns) {
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

    int lengthVarFieldsInCurrentRecordsToRead;
    recordsReadInCurrentPass = 0;
    boolean rowGroupFinished = false;
    boolean pageFinished = false;
    long valueLength;

    for (UnknownLengthColumn col : columns) {
      col.setup();
    }
    do {
      for (UnknownLengthColumn col : columns) {
        col.beginLoop();
      }
      lengthVarFieldsInCurrentRecordsToRead = 0;
      // find a run of values that can be read
      while (recordsReadInCurrentPass + columns.get(0).getCountOfRecordsToRead() < recordsToReadInThisPass){
        pageFinished = false;
        for (UnknownLengthColumn col : columns) {
          valueLength = col.checkNextRecord();
          if (valueLength == ROW_GROUP_FINISHED ){
            rowGroupFinished = true;
            break;
          }
          else if (valueLength == PAGE_FINISHED){
            // stop this loop when a page finishes, as we do not keep multiple uncompressed pages in memory
            pageFinished = true;
            break;
          }
          lengthVarFieldsInCurrentRecordsToRead += col.getCurrentValueLength();
        }
        // check that the next record will fit in the batch
        if (rowGroupFinished || (recordsReadInCurrentPass + 1) * parentReader.getBitWidthAllFixedFields() +
            lengthVarFieldsInCurrentRecordsToRead > parentReader.getBatchSize()){
          break;
        }
        else if (pageFinished){
          break;
        }
        else{
          // record the length of the current value in the value vector
          for (UnknownLengthColumn col : columns) {
            col.recordLengthCurrentRecord();
          }
        }
      }
      if (columns.get(0).getCountOfRecordsToRead() == 0){
        return 0;
      }
      recordsReadInCurrentPass += columns.get(0).getCountOfRecordsToRead();
      long posBefore = columns.get(0).pageReadStatus.readPosInBytes;
      for (UnknownLengthColumn col : columns) {
        col.readRecord();
      }
      if ( posBefore != columns.get(0).pageReadStatus.readPosInBytes){
        Math.min(2,8);
      }
    } while (recordsReadInCurrentPass < recordsToReadInThisPass);

    for (UnknownLengthColumn col : columns) {
      col.afterReading(recordsReadInCurrentPass);
    }
    return recordsReadInCurrentPass;
  }
}