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

public class VectorDataProviders {
  public interface VectorDataProvider<E> extends DrillDataStore {

    public E getDataSource();
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
    public int readValues(int valuesToRead, VectorDataReceivers.ByteBufBackedReceiver dest);

    public int readValues(int valuesToRead, VectorDataReceivers.ByteArrayBackedReceiver dest);

    public boolean hasMoreData();
  }

  public interface ByteArrayBackedProvider extends VectorDataProvider<byte[]>{

  }

  public interface ByteBufBackedProvider extends VectorDataProvider<ByteBuf>{

  }
}
