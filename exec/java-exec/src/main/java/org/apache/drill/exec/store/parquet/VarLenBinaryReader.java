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
  private static final int ROW_GROUP_FINISHED = -1;

  public VarLenBinaryReader(ParquetRecordReader parentReader, List<UnknownLengthColumn> columns) {
    this.parentReader = parentReader;
    this.columns = columns;
  }

  public static abstract class UnknownLengthColumn extends ColumnReader {

    UnknownLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
    }

    public abstract void setup();

    /**
     * Determine the length of the next record.
     *
     * @return - the length of the record, or -1 if the end of a row group has been reached
     * @throws IOException
     */
    public abstract int checkNextRecord() throws IOException;

    public abstract void readRecord() throws IOException;

    /**
     * Called after read loop completes, used to set vector-level status, clean up extra resources
     * if necessary.
     *
     * @param recordsReadInCurrentPass - the number or records just read
     */
    public abstract void afterReading(long recordsReadInCurrentPass);

    /**
     * Return the current record length, will be the same as the value last returned from
     * checkNextRecord.
     *
     * @return - length of the current record
     */
    public abstract int getCurrentRecordLength();
  }

  public static class VarLengthColumn extends UnknownLengthColumn {

    byte[] tempBytes;
    VarBinaryVector tempCurrVec;
    int byteLengthCurrentData;

    VarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setup() {
      // write the first 0 offset
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      tempCurrVec.getAccessor().getOffsetVector().getData().writeInt(0);
      bytesReadInCurrentPass = 0;
      valuesReadInCurrentPass = 0;
    }

    @Override
    public int checkNextRecord() throws IOException {
      if (pageReadStatus.currentPage == null
          || pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
        totalValuesRead += pageReadStatus.valuesRead;
        if (!pageReadStatus.next()) {
          return ROW_GROUP_FINISHED;
        }
      }
      tempBytes = pageReadStatus.pageDataByteArray;

      // re-purposing this field here for length in BYTES to prevent repetitive multiplication/division
      byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
          (int) pageReadStatus.readPosInBytes);
      return byteLengthCurrentData;
    }

    @Override
    public void readRecord() {
      tempBytes = pageReadStatus.pageDataByteArray;
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
      tempCurrVec.getAccessor().getOffsetVector().getData().writeInt((int) bytesReadInCurrentPass +
          getCurrentRecordLength() - 4 * (int) valuesReadInCurrentPass);
      tempCurrVec.getData().writeBytes(tempBytes, (int) pageReadStatus.readPosInBytes + 4,
          getCurrentRecordLength());
      pageReadStatus.readPosInBytes += getCurrentRecordLength() + 4;
      bytesReadInCurrentPass += getCurrentRecordLength() + 4;
      pageReadStatus.valuesRead++;
      valuesReadInCurrentPass++;
    }

    @Override
    public void afterReading(long recordsReadInCurrentPass) {
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      tempCurrVec.getMutator().setValueCount((int)recordsReadInCurrentPass);
    }

    @Override
    public int getCurrentRecordLength() {
      return byteLengthCurrentData;
    }
  }

  public static class NullableVarLengthColumn extends UnknownLengthColumn{

    byte[] tempBytes;
    VarBinaryVector tempCurrVec;
    NullableVarBinaryVector tempCurrNullVec;
    int byteLengthCurrentData;
    int nullsRead;
    boolean currentValNull = false;

    NullableVarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setup() {
      tempCurrNullVec = (NullableVarBinaryVector) valueVecHolder.getValueVector();
      tempCurrNullVec.getMutator().getVectorWithValues().getAccessor().getOffsetVector().getData().writeInt(0);
      bytesReadInCurrentPass = 0;
      valuesReadInCurrentPass = 0;
      nullsRead = 0;
    }

    @Override
    public int checkNextRecord() throws IOException {
      if (pageReadStatus.currentPage == null
          || pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
        totalValuesRead += pageReadStatus.valuesRead;
        if (!pageReadStatus.next()) {
          return -1;
        }
      }
      tempBytes = pageReadStatus.pageDataByteArray;
      if ( columnDescriptor.getMaxDefinitionLevel() > pageReadStatus.definitionLevels.readInteger()){
        currentValNull = true;
        byteLengthCurrentData = 0;
        nullsRead++;
        return 0;// field is null, no length to add to data vector
      }

      // re-purposing  this field here for length in BYTES to prevent repetitive multiplication/division
      byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
          (int) pageReadStatus.readPosInBytes);
      return byteLengthCurrentData;
    }

    @Override
    public void readRecord() throws IOException {
      tempBytes = pageReadStatus.pageDataByteArray;
      tempCurrNullVec = (NullableVarBinaryVector) valueVecHolder.getValueVector();
      // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
      tempCurrNullVec.getMutator().getVectorWithValues().getAccessor().getOffsetVector().getData()
          .writeInt(
              (int) bytesReadInCurrentPass  +
                  getCurrentRecordLength() - 4 * (valuesReadInCurrentPass -
                  (currentValNull ? Math.max (0, nullsRead - 1) : nullsRead)));
      currentValNull = false;
      if (getCurrentRecordLength() > 0){
        tempCurrNullVec.getData().writeBytes(tempBytes, (int) pageReadStatus.readPosInBytes + 4,
            getCurrentRecordLength());
        ((NullableVarBinaryVector)valueVecHolder.getValueVector()).getMutator().setIndexDefined(valuesReadInCurrentPass);
      }
      if (getCurrentRecordLength() > 0){
        pageReadStatus.readPosInBytes += getCurrentRecordLength() + 4;
        bytesReadInCurrentPass += getCurrentRecordLength() + 4;
      }
      pageReadStatus.valuesRead++;
      valuesReadInCurrentPass++;
      // reached the end of a page
      if ( pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
        pageReadStatus.next();
      }
    }

    @Override
    public void afterReading(long recordsReadInCurrentPass) {
      tempCurrNullVec = (NullableVarBinaryVector) valueVecHolder.getValueVector();
      tempCurrNullVec.getMutator().setValueCount((int)recordsReadInCurrentPass);
    }

    @Override
    public int getCurrentRecordLength() {
      return byteLengthCurrentData;
    }
  }

  public static class RepeatedByteAlignedColumn extends UnknownLengthColumn {

    byte[] tempBytes;
    VarBinaryVector tempCurrVec;
    int byteLengthCurrentData;

    RepeatedByteAlignedColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setup() {
      // write the first 0 offset
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      tempCurrVec.getAccessor().getOffsetVector().getData().writeInt(0);
      bytesReadInCurrentPass = 0;
      valuesReadInCurrentPass = 0;
    }

    @Override
    public int checkNextRecord() throws IOException {
      if (pageReadStatus.currentPage == null
          || pageReadStatus.valuesRead == pageReadStatus.currentPage.getValueCount()) {
        totalValuesRead += pageReadStatus.valuesRead;
        if (!pageReadStatus.next()) {
          return ROW_GROUP_FINISHED;
        }
      }
      tempBytes = pageReadStatus.pageDataByteArray;

      // re-purposing this field here for length in BYTES to prevent repetitive multiplication/division
      byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
          (int) pageReadStatus.readPosInBytes);
      return byteLengthCurrentData;
    }

    @Override
    public void readRecord() {
      tempBytes = pageReadStatus.pageDataByteArray;
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      // again, I am re-purposing the unused field here, it is a length n BYTES, not bits
      tempCurrVec.getAccessor().getOffsetVector().getData().writeInt((int) bytesReadInCurrentPass +
          getCurrentRecordLength() - 4 * (int) valuesReadInCurrentPass);
      tempCurrVec.getData().writeBytes(tempBytes, (int) pageReadStatus.readPosInBytes + 4,
          getCurrentRecordLength());
      pageReadStatus.readPosInBytes += getCurrentRecordLength() + 4;
      bytesReadInCurrentPass += getCurrentRecordLength() + 4;
      pageReadStatus.valuesRead++;
      valuesReadInCurrentPass++;
    }

    @Override
    public void afterReading(long recordsReadInCurrentPass) {
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      tempCurrVec.getMutator().setValueCount((int)recordsReadInCurrentPass);
    }

    @Override
    public int getCurrentRecordLength() {
      return byteLengthCurrentData;
    }
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

    int lengthVarFieldsInCurrentRecord;
    recordsReadInCurrentPass = 0;
    boolean rowGroupFinished = false;

    // same for the nullable columns
    for (UnknownLengthColumn col : columns) {
      col.setup();
    }
    do {
      lengthVarFieldsInCurrentRecord = 0;
      for (UnknownLengthColumn col : columns) {
        if (col.checkNextRecord() == ROW_GROUP_FINISHED ){
          rowGroupFinished = true;
          break;
        }
        lengthVarFieldsInCurrentRecord += col.getCurrentRecordLength();
      }
      // check that the next record will fit in the batch
      if (rowGroupFinished || (recordsReadInCurrentPass + 1) * parentReader.getBitWidthAllFixedFields() + lengthVarFieldsInCurrentRecord
          > parentReader.getBatchSize()){
        break;
      }
      else{
        recordsReadInCurrentPass++;
      }
      for (UnknownLengthColumn col : columns) {
        col.readRecord();
      }
    } while (recordsReadInCurrentPass < recordsToReadInThisPass);

    for (UnknownLengthColumn col : columns) {
      col.afterReading(recordsReadInCurrentPass);
    }
    return recordsReadInCurrentPass;
  }
}