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

import java.io.IOException;

public abstract class DataSourceTransformers {

  public abstract class DataSourceTransformer<SOURCE extends VectorDataProviders.VectorDataProvider,
      DEST extends VectorDataReceivers.VectorDataReceiver> {

    SOURCE source;
    DEST destination;

    public DataSourceTransformer( SOURCE source,
                                  DEST destination){

    }

    public void readAllFixedFields(long recordsToReadInThisPass, ColumnReaderParquet firstColumnStatus) throws IOException {
      int valuesJustRead;
      do {
        // if no page has been read, or all of the records have been read out of a page, read the next one
        if (source.needNewSubComponent()){
          if (!source.getNextSubComponent()) {
            break;
          }
        }

        valuesJustRead = (int) recordsToReadInThisPass - readValues((int) recordsToReadInThisPass, destination);

//        setValuesReadInCurrentPass(getValuesReadInCurrentPass() + (int) getRecordsReadInThisIteration());
//        setTotalValuesRead(getTotalValuesRead() + (int) getRecordsReadInThisIteration());
      }
      while (true);//getValuesReadInCurrentPass() < recordsToReadInThisPass && getPageReadStatus().hasMoreData());
//      getValueVecHolder().getValueVector().getMutator().setValueCount(
//          getValuesReadInCurrentPass());
    }

    public abstract int readValues(int recordsToRead, DEST destination);
  }

  public class ArrayToByteBufTransformer extends DataSourceTransformer<VectorDataProviders.ByteArrayBackedProvider,
      VectorDataReceivers.ByteBufBackedReceiver>{

    public ArrayToByteBufTransformer(VectorDataProviders.ByteArrayBackedProvider byteArrayBackedProvider,
                                     VectorDataReceivers.ByteBufBackedReceiver destination) {
      super(byteArrayBackedProvider, destination);
    }

    @Override
    public int readValues(int recordsToRead, VectorDataReceivers.ByteBufBackedReceiver destination) {
      return 1;
    }

  }

}
