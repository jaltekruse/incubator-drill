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
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;

public abstract class UnknownLengthColumn extends ColumnReader {

  UnknownLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
  }

  public abstract void setup() throws IOException;

  /**
   * Determine the length of the next record.
   *
   * @return - the length of the record, or -1 if the end of a row group has been reached
   * @throws IOException
   */
  public abstract int checkNextRecord() throws IOException;

  /**
   * Stores the length of this value in its respective location in the value vector. In variable length columns
   * it will be storing the length of the single value. For repeated it will store the number of values
   * that appear in this column of the current record.
   *
   * To be called after all variable length columns have been checked to see that they fit in the current vector.
   *
   * @throws IOException
   */
  public abstract void recordLengthCurrentRecord() throws IOException;

  public abstract void readRecord() throws IOException;

  /**
   * Called after read loop completes, used to set vector-level status, clean up extra resources
   * if necessary.
   *
   * @param recordsReadInCurrentPass - the number or records just read
   */
  public abstract void afterReading(long recordsReadInCurrentPass);

  /**
   * Return the current value length, will be the same as the value last returned from
   * checkNextRecord.
   *
   * @return - length of the current value (or list of repeated values)
   */
  public abstract int getCurrentValueLength();

  public abstract int getCountOfRecordsToRead();

  /**
   * Return the total number of bytes to read. Can represent data for one or
   * more records in the dataset. Allows for optimized reading of longer
   * strings of values.
   * @return the length in bytes to read
   */
  public abstract int getTotalReadLength();

  public abstract int beginLoop() throws IOException;
}

