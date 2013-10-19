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
package org.apache.drill.exec.store.reader_writer;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public final class VectorDataUtils {

  public interface BitProvider {

    public void setup();

    public boolean next();

    public int lengthRunOfTrueVals();

    public int lengthRunOfFalseVals();
  }

  // methods for compressing/decompressing vectors of nullable values
  // ValueVectors feature spaced out null values to allow for quick random access
  // On disk formats do not leave such spaces, as they are focused on compactness first and foremost

  /**
   * Spread out null values in a vector of values.
   *
   * @param src - buffer of tightly packed data, nulls do not take up space in the primary data
   * @param dest - buffer to store all of the spaced out data
   * @param defintions - structure to provide the definitions/nullability of each value
   */
  public static void spaceNulls(ByteBuf src, ByteBuf dest, BitProvider defintions){

  }

  public static void transferBytes(Object src, int srcPos, Object dest, int destPos, int len) throws IOException {
    throw new IOException("Imporoper use of method, must cast down to use one of the implementations " +
        "for particular types");
  }

  public static void transferBytes(byte[] src, int srcPos, byte[] dest, int destPos, int len){
    System.arraycopy(src, srcPos, dest, destPos, len);
  }

}
