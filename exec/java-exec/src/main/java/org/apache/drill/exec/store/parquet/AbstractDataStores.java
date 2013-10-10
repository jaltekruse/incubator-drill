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
package org.apache.drill.exec.store.parquet;

import io.netty.buffer.ByteBuf;
import org.apache.drill.exec.store.VectorHolder;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.ValueVector;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;
import parquet.column.values.ValuesReader;
import parquet.hadoop.metadata.ColumnChunkMetaData;

public class AbstractDataStores {

  /**
   * An AbstractDataStore represents a storage mechanism in memory or on disk
   * that can provide data to another source or have data written into it. As
   * Drill will be concerned with reading data in many different formats, this class
   * attempts to encapsulate all of the functionality needed for each source/sink for
   * data.
   */
  public static class AbstractDataStore {

  }

  public static class ValueVectorWriter extends AbstractDataStore implements VectorDataReceiver<ByteBuf> {

    int currentValue;
    int dataTypeLengthInBytes;
    BaseValueVector vector;

    @Override
    public ByteBuf getDataDestination() {
      return vector.getData();
    }

    @Override
    public void receiveData(ParquetPage source, int valuesToRead, int sourcePos) {
      vector.getData().writeBytes(source.getData(),
          sourcePos, valuesToRead * dataTypeLengthInBytes);

    }

    @Override
    public void updatePositionAfterWrite(int valsWritten) {
      currentValue += valsWritten;
    }
  }

  public static class ParquetRowGroupReader extends AbstractDataStore {


  }

  public interface VectorDataReceiver<E> {

    public E getDataDestination();

    public void receiveData(ParquetPage source, int valuesToRead, int sourcePos);

    public void updatePositionAfterWrite(int valsWritten);

  }

  public interface VectorDataProvider<E> {
    public boolean needNewSubComponent();
    public E getData();
    public int valuesLeft();
    //public void transferValuesToVector(ByteBuf buf, int values);

    /**
     * Method to read the specified number of values out of this part of the file.
     *
     * @param valuesToRead - the number of values to read
     * @param dest - the destination of the data
     * @return - the number of records that still need to be read in the next part of the file.
     *           valuesToRead - valuesLeft() at the time the method was called
     */
    public int readValues(int valuesToRead, VectorDataReceiver dest);
  }

  // TODO - do I need an interface to show that this is an entry point for reading/writing?
  public static class ParquetColumnChunk extends AbstractDataStore {

// Value Vector for this column
    private VectorHolder valueVecHolder;
    // column description from the parquet library
    private ColumnDescriptor columnDescriptor;
    // metadata of the column, from the parquet library
    private ColumnChunkMetaData columnChunkMetaData;
    // status information on the current page
    private PageReadStatus pageReadStatus;

    private long readPositionInBuffer;

    // quick reference to see if the field is fixed length (as this requires an instanceof)
    private boolean isFixedLength;
    // counter for the total number of values read from one or more pages
    // when a batch is filled all of these values should be the same for each column
    private int totalValuesRead;
    // counter for the values that have been read in this pass (a single call to the next() method)
    private int valuesReadInCurrentPass;
    // length of single data value in bits, if the length is fixed
    int dataTypeLengthInBytes;
    private int bytesReadInCurrentPass;
    private ParquetRecordReader parentReader;

    private ByteBuf vectorData;

    // variables for a single read pass
    private long readStartInBytes = 0;
    private long readLength = 0;
    private long readLengthInBits = 0;
    private long recordsReadInThisIteration = 0;


  }


  public static class ParquetPage extends AbstractDataStore implements VectorDataProvider<byte[]> {

    ParquetColumnChunk parentColumnChunk;

    // store references to the pages that have been uncompressed, but not copied to ValueVectors yet
    Page currentPage;
    // buffer to store bytes of current page, set to max size of parquet page
    byte[] pageDataByteArray = new byte[ParquetRecordReader.PARQUET_PAGE_MAX_SIZE];

    // read position in the current page, stored in the ByteBuf in ParquetRecordReader called bufferWithAllData
    long readPosInBytes;
    // the number of values read out of the current page
    int valuesRead;
    int byteLength;
    int rowGroupIndex;
    ValuesReader definitionLevels;
    ValuesReader valueReader;


    /**
     * Method to read the specified number of values out of this
     * @param valuesToRead
     * @return
     */
    public int readValues(int valuesToRead, VectorDataReceiver dest){
      int numValues = Math.min(valuesLeft(), valuesToRead);

      dest.receiveData(this, numValues, (int) readPosInBytes);
      valuesRead += numValues;

      return valuesToRead - numValues;
    }

    public boolean needNewSubComponent(){
      return (currentPage == null || valuesRead == currentPage.getValueCount());
    }

    @Override
    public byte[] getData() {
      return pageDataByteArray;
    }

    public int valuesLeft(){
      return currentPage.getValueCount() - valuesRead;
    }

    public void transferValuesToVector(Object o, int values){
      throw new RuntimeException("Data source did not define a type for the destination buffer.");
    }

//    public void transferValuesToVector(ByteBuf buf, int values){
//      buf.writeBytes(pageDataByteArray,
//          (int) readPosInBytes, values * parentColumnChunk.dataTypeLengthInBytes);
//    }
  }
}
