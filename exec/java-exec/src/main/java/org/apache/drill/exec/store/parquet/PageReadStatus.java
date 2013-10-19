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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import parquet.bytes.BytesInput;
import parquet.column.ValuesType;
import parquet.column.page.Page;
import parquet.column.values.ValuesReader;
import parquet.format.PageHeader;

import java.io.IOException;

import static parquet.format.Util.readPageHeader;

// class to keep track of the read position of variable length columns
public final class PageReadStatus implements VectorDataProvider<byte[]>, DrillDataStore {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PageReadStatus.class);

  private ColumnReaderParquet parentColumnReader;

  // store references to the pages that have been uncompressed, but not copied to ValueVectors yet
  private Page currentPage;
  // buffer to store bytes of current page, set to max size of parquet page
  private byte[] pageDataByteArray = new byte[ParquetRecordReader.PARQUET_PAGE_MAX_SIZE];

  // read position in the current page, stored in the ByteBuf in ParquetRecordReader called bufferWithAllData
  private long readPosInBytes;
  // storage space for extra bits at the end of a page if they did not line up with a byte boundary
  // prevents the need to keep the entire last page, as these pageDataByteArray need to be added to the next batch
  //byte extraBits;
  // the number of values read out of the last page
  private int valuesRead;
  private int byteLength;
  private int rowGroupIndex;
  private ValuesReader definitionLevels;
  private ValuesReader valueReader;

  PageReadStatus(ColumnReaderParquet parentStatus, int rowGroupIndex, ByteBuf bufferWithAllData){
    this.setParentColumnReader(parentStatus);
    this.setRowGroupIndex(rowGroupIndex);
  }

  /**
   * Grab the next page.
   *
   * @return - if another page was present
   * @throws java.io.IOException
   */
  public boolean next() throws IOException {

    int shift = 0;
    if (getRowGroupIndex() == 0) shift = 0;
    else shift = 4;
    // first ROW GROUP has a different endpoint, because there are for bytes at the beginning of the file "PAR1"
    if (getParentColumnReader().getReadPositionInBuffer() + shift == getParentColumnReader().getColumnChunkMetaData().getFirstDataPageOffset() + getParentColumnReader().getColumnChunkMetaData().getTotalSize()){
      return false;
    }
    // TODO - in the JIRA for parquet steven put a stack trace for an error with a row group with 3 values in it
    // the Math.min with the end of the buffer should fix it but now I'm not getting results back, leaving it here for now
    // because it is needed, but there might be a problem with it
    ByteBufInputStream f = new ByteBufInputStream(getParentColumnReader().getParentReader().getBufferWithAllData().slice(
        (int) getParentColumnReader().getReadPositionInBuffer(),
        Math.min(200, getParentColumnReader().getParentReader().getBufferWithAllData().capacity() - (int) getParentColumnReader().getReadPositionInBuffer())));
    int before = f.available();
    PageHeader pageHeader = readPageHeader(f);
    int length = before - f.available();
    f = new ByteBufInputStream(getParentColumnReader().getParentReader().getBufferWithAllData().slice(
        (int) getParentColumnReader().getReadPositionInBuffer() + length, pageHeader.getCompressed_page_size()));

    BytesInput bytesIn = getParentColumnReader().getParentReader().getCodecFactoryExposer()
        .decompress(BytesInput.from(f, pageHeader.compressed_page_size), pageHeader.getUncompressed_page_size(),
            getParentColumnReader().getColumnChunkMetaData().getCodec());
    setCurrentPage(new Page(
        bytesIn,
        pageHeader.data_page_header.num_values,
        pageHeader.uncompressed_page_size,
        ParquetStorageEngine.parquetMetadataConverter.getEncoding(pageHeader.data_page_header.repetition_level_encoding),
        ParquetStorageEngine.parquetMetadataConverter.getEncoding(pageHeader.data_page_header.definition_level_encoding),
        ParquetStorageEngine.parquetMetadataConverter.getEncoding(pageHeader.data_page_header.encoding)
    ));

    getParentColumnReader().setReadPositionInBuffer(getParentColumnReader().getReadPositionInBuffer() + pageHeader.compressed_page_size + length);
    setByteLength(pageHeader.uncompressed_page_size);

    if (getCurrentPage() == null) {
      return false;
    }

    // if the buffer holding each page's data is not large enough to hold the current page, re-allocate, with a little extra space
    if (pageHeader.getUncompressed_page_size() > getPageDataByteArray().length) {
      setPageDataByteArray(new byte[pageHeader.getUncompressed_page_size() + 100]);
    }
    // TODO - would like to get this into the mainline, hopefully before alpha
    setPageDataByteArray(getCurrentPage().getBytes().toByteArray());

    setReadPosInBytes(0);
    if (getParentColumnReader().getColumnDescriptor().getMaxDefinitionLevel() != 0){
      setDefinitionLevels(getCurrentPage().getDlEncoding().getValuesReader(getParentColumnReader().getColumnDescriptor(), ValuesType.DEFINITION_LEVEL));
      setValueReader(getCurrentPage().getValueEncoding().getValuesReader(getParentColumnReader().getColumnDescriptor(), ValuesType.VALUES));
      int endOfDefinitionLevels = getDefinitionLevels().initFromPage(getCurrentPage().getValueCount(), getPageDataByteArray(), 0);
      getValueReader().initFromPage(getCurrentPage().getValueCount(), getPageDataByteArray(), endOfDefinitionLevels);
      setReadPosInBytes(endOfDefinitionLevels);
    }

    setValuesRead(0);
    return true;
  }

  @Override
  public boolean needNewSubComponent() {
    // if no page has been read, or all of the records have been read out of a page, read the next one
    return getCurrentPage() == null || getValuesRead() == getCurrentPage().getValueCount();
  }

  @Override
  public boolean getNextSubComponent() throws IOException {
    return next();
  }

  /**
   * Method to read the specified number of values out of this
   * @param valuesToRead
   * @return
   */
  public int readValues(int valuesToRead, VectorDataReceiver dest){
    int numValues = Math.min(valuesLeft(), valuesToRead);

    dest.receiveData(this, numValues, (int) readPosInBytes);
    dest.updatePositionAfterWrite(numValues);
    this.updatePositionAfterWrite(numValues);

    return valuesToRead - numValues;
  }

  @Override
  public byte[] getData() {
    return pageDataByteArray;
  }

  public int valuesLeft(){
    return currentPage.getValueCount() - valuesRead;
  }

  @Override
  public boolean hasSubComponents() {
    return true;
  }

  @Override
  public DrillDataStore getCurrentSubComponent() {
    return this;
  }

  @Override
  public boolean dataStoredAtThisLevel() {
    return true;
  }

  @Override
  public void updatePositionAfterWrite(int valsWritten) {
    valuesRead += valsWritten;
    //setReadPosInBytes(getReadStartInBytes() + getReadLength());
  }

  @Override
  public boolean finishedProcessing() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public ColumnReaderParquet getParentColumnReader() {
    return parentColumnReader;
  }

  public void setParentColumnReader(ColumnReaderParquet parentColumnReader) {
    this.parentColumnReader = parentColumnReader;
  }

  public Page getCurrentPage() {
    return currentPage;
  }

  public void setCurrentPage(Page currentPage) {
    this.currentPage = currentPage;
  }

  public byte[] getPageDataByteArray() {
    return pageDataByteArray;
  }

  public void setPageDataByteArray(byte[] pageDataByteArray) {
    this.pageDataByteArray = pageDataByteArray;
  }

  public long getReadPosInBytes() {
    return readPosInBytes;
  }

  public void setReadPosInBytes(long readPosInBytes) {
    this.readPosInBytes = readPosInBytes;
  }

  public int getValuesRead() {
    return valuesRead;
  }

  public void setValuesRead(int valuesRead) {
    this.valuesRead = valuesRead;
  }

  public int getByteLength() {
    return byteLength;
  }

  public void setByteLength(int byteLength) {
    this.byteLength = byteLength;
  }

  public int getRowGroupIndex() {
    return rowGroupIndex;
  }

  public void setRowGroupIndex(int rowGroupIndex) {
    this.rowGroupIndex = rowGroupIndex;
  }

  public ValuesReader getDefinitionLevels() {
    return definitionLevels;
  }

  public void setDefinitionLevels(ValuesReader definitionLevels) {
    this.definitionLevels = definitionLevels;
  }

  public ValuesReader getValueReader() {
    return valueReader;
  }

  public void setValueReader(ValuesReader valueReader) {
    this.valueReader = valueReader;
  }

}
