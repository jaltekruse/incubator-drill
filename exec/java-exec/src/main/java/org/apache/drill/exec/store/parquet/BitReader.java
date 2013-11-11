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

import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.ValueVector;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.ColumnChunkMetaData;

public final class BitReader extends ColumnReaderParquet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitReader.class);

  byte currentByte;
  byte nextByte;

  // bit shift needed for the next page if the last one did not line up with a byte boundary
  int bitShift;

  BitReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor, ColumnChunkMetaData columnChunkMetaData,
            boolean fixedLength, ValueVector v) {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v);
  }

  @Override
  protected void readField(long recordsToReadInThisPass) {

    setRecordsReadInThisIteration(Math.min(getPageReadStatus().getCurrentPage().getValueCount()
        - getPageReadStatus().getValuesRead(), recordsToReadInThisPass - getValuesReadInCurrentPass()));

    setReadStartInBytes(getPageReadStatus().getReadPosInBytes());
    setReadLengthInBits(getRecordsReadInThisIteration() * getDataTypeLengthInBits());
    setReadLength((int) Math.ceil(getReadLengthInBits() / 8.0));

    byte[] bytes;
    bytes = getPageReadStatus().getPageDataByteArray();
    // standard read, using memory mapping
    if (bitShift == 0) {
      ((BaseDataValueVector) getValueVecHolder().getValueVector()).getData().writeBytes(bytes,
          (int) getReadStartInBytes(), (int) getReadLength());
    } else { // read in individual values, because a bitshift is necessary with where the last page or batch ended

      setVectorData(((BaseDataValueVector) getValueVecHolder().getValueVector()).getData());
      nextByte = bytes[(int) Math.max(0, Math.ceil(getPageReadStatus().getValuesRead() / 8.0) - 1)];
      setReadLengthInBits(getRecordsReadInThisIteration() + bitShift);

      int i = 0;
      // read individual bytes with appropriate shifting
      for (; i < (int) getReadLength(); i++) {
        currentByte = nextByte;
        currentByte = (byte) (currentByte >>> bitShift);
        // mask the bits about to be added from the next byte
        currentByte = (byte) (currentByte & ParquetRecordReader.startBitMasks[bitShift - 1]);
        // if we are not on the last byte
        if ((int) Math.ceil(getPageReadStatus().getValuesRead() / 8.0) + i < getPageReadStatus().getByteLength()) {
          // grab the next byte from the buffer, shift and mask it, and OR it with the leftover bits
          nextByte = bytes[(int) Math.ceil(getPageReadStatus().getValuesRead() / 8.0) + i];
          currentByte = (byte) (currentByte | nextByte
              << (8 - bitShift)
              & ParquetRecordReader.endBitMasks[8 - bitShift - 1]);
        }
        getVectorData().setByte(getValuesReadInCurrentPass() / 8 + i, currentByte);
      }
      getVectorData().setIndex(0, (getValuesReadInCurrentPass() / 8)
          + (int) getReadLength() - 1);
      getVectorData().capacity(getVectorData().writerIndex() + 1);
    }

    getPageReadStatus().setValuesRead(getValuesReadInCurrentPass() + (int) getRecordsReadInThisIteration());

    // check if the values in this page did not end on a byte boundary, store a number of bits the next page must be
    // shifted by to read all of the values into the vector without leaving space
    if (getReadLengthInBits() % 8 != 0) {
      bitShift = (int) getReadLengthInBits() % 8;
    } else {
      bitShift = 0;
    }
  }
}
