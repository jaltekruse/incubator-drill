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
import parquet.column.ColumnDescriptor;
import parquet.format.Encoding;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.io.api.Binary;

import java.io.IOException;

public abstract class VarLengthColumn<V extends ValueVector> extends ColumnReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(VarLengthColumn.class);

  Binary currDictVal;

  VarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                  ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v,
                  SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
      if (columnChunkMetaData.getEncodings().contains(Encoding.PLAIN_DICTIONARY)) {
        usingDictionary = true;
      }
      else {
        usingDictionary = false;
      }
  }

  /**
   * Determines the size of a single value in a variable column.
   *
   * Return value indicates if we have finished a row group and should stop reading
   *
   * @param recordsReadInCurrentPass
   * @param lengthVarFieldsInCurrentRecord
   * @return - true if we should stop reading
   * @throws IOException
   */
  public boolean determineSize(long recordsReadInCurrentPass, Integer lengthVarFieldsInCurrentRecord) throws IOException {

    if (recordsReadInCurrentPass == valueVec.getValueCapacity())
      return true;

    boolean doneReading = readPage();
    if (doneReading)
      return true;

    doneReading = readAndStoreValueSizeInformation();
    if (doneReading)
      return true;

    lengthVarFieldsInCurrentRecord += dataTypeLengthInBits;

    doneReading = checkVectorCapacityReached();
    if (doneReading)
      return true;

    return false;
  }

  protected void readRecords(int recordsToRead) {
    for (int i = 0; i < recordsToRead; i++) {
      readField(i, null);
    }
    pageReadStatus.valuesRead += recordsToRead;
  }

  protected abstract boolean readAndStoreValueSizeInformation() throws IOException;

  public abstract void updatePosition();

  public abstract void updateReadyToReadPosition();

  public void reset() {
    bytesReadInCurrentPass = 0;
    valuesReadInCurrentPass = 0;
    pageReadStatus.valuesReadyToRead = 0;
  }

  public abstract boolean setSafe(int index, byte[] bytes, int start, int length);

  public abstract int capacity();

  public abstract boolean skipReadyToReadPositionUpdate();

  // Read a page if we need more data, returns true if we need to exit the read loop
  public boolean readPage() throws IOException {
    if (pageReadStatus.currentPage == null
        || totalValuesReadAndReadyToReadInPage() == pageReadStatus.currentPage.getValueCount()) {
      readRecords(pageReadStatus.valuesReadyToRead);
      if (pageReadStatus.currentPage != null)
        totalValuesRead += pageReadStatus.currentPage.getValueCount();
      if (!pageReadStatus.next()) {
        hitRowGroupEnd();
        return true;
      }
      postPageRead();
    }
    return false;
  }

  protected int totalValuesReadAndReadyToReadInPage() {
    return pageReadStatus.valuesRead + pageReadStatus.valuesReadyToRead;
  }

  protected void postPageRead() {
    pageReadStatus.valuesReadyToRead = 0;
  }

  protected void hitRowGroupEnd() {}

  protected boolean checkVectorCapacityReached() {
    if (bytesReadInCurrentPass + dataTypeLengthInBits > capacity()) {
      // TODO - determine if we need to add this back somehow
      //break outer;
      logger.debug("Reached the capacity of the data vector in a variable length value vector.");
      return true;
    }
    return false;
  }
}
