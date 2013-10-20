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
import org.apache.drill.exec.store.BatchExceededException;
import org.apache.drill.exec.vector.*;

public class FixedByteAlignedVector implements VectorDataProviders.ByteBufBackedProvider, VectorDataReceivers.ByteBufBackedReceiver {
  private int count;
  private int groupCount;
  private int length;
  private ValueVector vector;
  private int currentLength;

  public FixedByteAlignedVector(int length, ValueVector vector) {
    this.length = length;
    this.vector = vector;
  }

  public FixedByteAlignedVector(ValueVector vector) {
    this.length = vector.getValueCapacity();
    this.vector = vector;
  }

  public ValueVector getValueVector() {
    return vector;
  }

  public void incAndCheckLength(int newLength) {
    if (!hasEnoughSpace(newLength)) {
      throw new BatchExceededException(length, vector.getBufferSize() + newLength);
    }

    currentLength += newLength;
    count += 1;
  }

  public void setGroupCount(int groupCount) {
    if (this.groupCount < groupCount) {
      RepeatedMutator mutator = (RepeatedMutator) vector.getMutator();
      while (this.groupCount < groupCount) {
        mutator.startNewGroup(++this.groupCount);
      }
    }
  }

  public boolean hasEnoughSpace(int newLength) {
    return length >= currentLength + newLength;
  }

  public int getLength() {
    return length;
  }

  public void reset() {
    currentLength = 0;
    count = 0;
    allocateNew(length);
  }

  public void populateVectorLength() {
    ValueVector.Mutator mutator = vector.getMutator();
    if (vector instanceof RepeatedFixedWidthVector || vector instanceof RepeatedVariableWidthVector) {
      mutator.setValueCount(groupCount);
    } else {
      mutator.setValueCount(count);
    }
  }

  public void allocateNew(int valueLength) {
    AllocationHelper.allocate(vector, valueLength, 10, 5);
  }

  public void allocateNew(int valueLength, int repeatedPerTop) {
    AllocationHelper.allocate(vector, valueLength, 10, repeatedPerTop);
  }

  @Override
  public ByteBuf getDataSource() {
    return ((BaseDataValueVector) vector).getData();
  }

  @Override
  public int valuesLeft() {
    return length - count;
  }

  @Override
  public int readValues(int valuesToRead, VectorDataReceivers.ByteBufBackedReceiver dest) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int readValues(int valuesToRead, VectorDataReceivers.ByteArrayBackedReceiver dest) {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean hasMoreData() {
    return valuesLeft() == 0;
  }

  @Override
  public ByteBuf getDataDestination() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void receiveData(VectorDataProviders.ByteArrayBackedProvider source, int valuesToRead, int sourcePos) {
    getDataSource().writeBytes(source.getDataSource(),
        sourcePos, valuesToRead * source.getTypeLengthInBytes());
  }

  @Override
  public void receiveData(VectorDataProviders.ByteBufBackedProvider source, int valuesToRead, int sourcePos) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean hasMoreSpace() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void updatePositionAfterWrite(int valsWritten) {
    count += valsWritten;
  }

  @Override
  public int getTypeLengthInBytes() throws UnsupportedOperationException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public int getTypeLengthInBits() throws UnsupportedOperationException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean needNewSubComponent() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean getNextSubComponent() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean hasSubComponents() {
    return false;
  }

  @Override
  public DrillDataStore getCurrentSubComponent() {
    return null;
  }

  @Override
  public boolean dataStoredAtThisLevel() {
    return true;
  }
}
