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

    public abstract void beginLoop();
  }

  // These functions are designed specifically for the variable length type available in parquet today. Unfortunately
  // it features length, value, length, value encoding of data that makes it exceedingly difficult to
  public static class VarLengthColumn extends UnknownLengthColumn {

    byte[] tempBytes;
    VarBinaryVector tempCurrVec;
    int byteLengthCurrentData;
    int totalValuesToRead;

    VarLengthColumn(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v) {
      super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
    }

    @Override
    protected void readField(long recordsToRead, ColumnReader firstColumnStatus) {
      throw new UnsupportedOperationException();
    }

    long initialReadPos;

    @Override
    public void setup() {
      // write the first 0 offset
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      tempCurrVec.getAccessor().getOffsetVector().getData().writeInt(0);
      bytesReadInCurrentPass = 0;
      valuesReadInCurrentPass = 0;
      totalValuesToRead = 0;
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

      byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
          (int) pageReadStatus.readPosInBytes);
      return byteLengthCurrentData;
    }

    @Override
    public void recordLengthCurrentRecord() throws IOException {
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      int length =  bytesReadInCurrentPass +
          getCurrentValueLength() - 4 * valuesReadInCurrentPass;
      tempCurrVec.getAccessor().getOffsetVector().getData().writeInt(length);
      totalValuesToRead++;
      bytesReadInCurrentPass += getCurrentValueLength() + 4;
      pageReadStatus.readPosInBytes += getCurrentValueLength() + 4;
      pageReadStatus.valuesRead++;
      valuesReadInCurrentPass++;
    }

    @Override
    public void readRecord() {
      valuesReadInCurrentPass = 0;
      // extra 4 for first offset that needs to be skipped
      pageReadStatus.readPosInBytes = initialReadPos + 4;
      tempBytes = pageReadStatus.pageDataByteArray;
      UInt4Vector.Accessor accesor = tempCurrVec.getAccessor().getOffsetVector().getAccessor();
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      for (int i = 0; i < totalValuesToRead; i++) {
        tempCurrVec.getData().writeBytes(tempBytes, (int) pageReadStatus.readPosInBytes,
            accesor.get(valuesReadInCurrentPass + 1) - accesor.get(valuesReadInCurrentPass));
        pageReadStatus.readPosInBytes += accesor.get(valuesReadInCurrentPass + 1) - accesor.get(valuesReadInCurrentPass) + 4;
        valuesReadInCurrentPass++;
      }
    }

    @Override
    public void afterReading(long recordsReadInCurrentPass) {
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      tempCurrVec.getMutator().setValueCount((int) recordsReadInCurrentPass);
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
      // TODO - this is supposed to be addressed soon by the Parquet team at twitter, unfortunately because the
      // length/value scheme was in the format, we will likely have to support it for quite some time, but
      // hopefully the new format with the data and lengths separated will become default when it is finished
      throw new UnsupportedOperationException("Current implementation of variable length columns prevents " +
          "reading multiple values at a time.");
    }

    @Override
    public void beginLoop() {
      initialReadPos = pageReadStatus.readPosInBytes;
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

      byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
          (int) pageReadStatus.readPosInBytes);
      return byteLengthCurrentData;
    }

    @Override
    public void recordLengthCurrentRecord() throws IOException {
      tempCurrNullVec = (NullableVarBinaryVector) valueVecHolder.getValueVector();
      tempCurrNullVec.getMutator().getVectorWithValues().getAccessor().getOffsetVector().getData()
          .writeInt(
              (int) bytesReadInCurrentPass  +
                  getCurrentValueLength() - 4 * (valuesReadInCurrentPass -
                  (currentValNull ? Math.max (0, nullsRead - 1) : nullsRead)));
    }

    @Override
    public void readRecord() throws IOException {
      tempBytes = pageReadStatus.pageDataByteArray;
      tempCurrNullVec = (NullableVarBinaryVector) valueVecHolder.getValueVector();
      currentValNull = false;
      if (getCurrentValueLength() > 0){
        tempCurrNullVec.getData().writeBytes(tempBytes, (int) pageReadStatus.readPosInBytes + 4,
            getCurrentValueLength());
        ((NullableVarBinaryVector)valueVecHolder.getValueVector()).getMutator().setIndexDefined(valuesReadInCurrentPass);
      }
      if (getCurrentValueLength() > 0){
        pageReadStatus.readPosInBytes += getCurrentValueLength() + 4;
        bytesReadInCurrentPass += getCurrentValueLength() + 4;
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
      tempCurrNullVec.getMutator().setValueCount((int) recordsReadInCurrentPass);
    }

    @Override
    public int getCurrentValueLength() {
      return byteLengthCurrentData;
    }

    @Override
    public int getCountOfRecordsToRead() {
      return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getTotalReadLength() {
      // TODO - see comment above in non-nullable varlength reader
      throw new UnsupportedOperationException("Current implementation of variable length columns prevents " +
          "reading multiple values at a time.");
    }

    @Override
    public void beginLoop() {
      //To change body of implemented methods use File | Settings | File Templates.
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

      byteLengthCurrentData = BytesUtils.readIntLittleEndian(tempBytes,
          (int) pageReadStatus.readPosInBytes);
      return byteLengthCurrentData;
    }

    @Override
    public void recordLengthCurrentRecord() throws IOException {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void readRecord() {
      tempBytes = pageReadStatus.pageDataByteArray;
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      tempCurrVec.getAccessor().getOffsetVector().getData().writeInt((int) bytesReadInCurrentPass +
          getCurrentValueLength() - 4 * (int) valuesReadInCurrentPass);
      tempCurrVec.getData().writeBytes(tempBytes, (int) pageReadStatus.readPosInBytes + 4,
          getCurrentValueLength());
      pageReadStatus.readPosInBytes += getCurrentValueLength() + 4;
      bytesReadInCurrentPass += getCurrentValueLength() + 4;
      pageReadStatus.valuesRead++;
      valuesReadInCurrentPass++;
    }

    @Override
    public void afterReading(long recordsReadInCurrentPass) {
      tempCurrVec = (VarBinaryVector) valueVecHolder.getValueVector();
      tempCurrVec.getMutator().setValueCount((int)recordsReadInCurrentPass);
    }

    @Override
    public int getCurrentValueLength() {
      return byteLengthCurrentData;
    }

    @Override
    public int getCountOfRecordsToRead() {
      return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getTotalReadLength() {
      return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void beginLoop() {
      //To change body of implemented methods use File | Settings | File Templates.
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

    int lengthVarFieldsInCurrentRecordsToRead;
    recordsReadInCurrentPass = 0;
    boolean rowGroupFinished = false;

    // same for the nullable columns
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
        for (UnknownLengthColumn col : columns) {
          if (col.checkNextRecord() == ROW_GROUP_FINISHED ){
            rowGroupFinished = true;
            break;
          }
          lengthVarFieldsInCurrentRecordsToRead += col.getCurrentValueLength();
        }
        // check that the next record will fit in the batch
        if (rowGroupFinished || (recordsReadInCurrentPass + 1) * parentReader.getBitWidthAllFixedFields() +
            lengthVarFieldsInCurrentRecordsToRead > parentReader.getBatchSize()){
          break;
        }
        else{
          // record the length of the current value in the value vector
          for (UnknownLengthColumn col : columns) {
            col.recordLengthCurrentRecord();
          }
        }
      }
      recordsReadInCurrentPass += columns.get(0).getCountOfRecordsToRead();
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