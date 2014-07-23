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

package parquet.io.api;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.google.common.base.Charsets;

public class ByteBufBinary extends Binary {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ByteBufBinary.class);

  private final ByteBuf buf;

  public ByteBufBinary(ByteBuf buf) {
    super();
    this.buf = buf;
  }

  @Override
  public String toStringUsingUTF8() {
    return buf.toString(Charsets.UTF_8);
  }

  @Override
  public int length() {
    return buf.capacity();
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    buf.getBytes(0, out, length());
  }

  @Override
  public void writeTo(DataOutput paramDataOutput) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBytes() {
    byte[] b = new byte[buf.capacity()];
    buf.getBytes(0,  b);
    return b;
  }

  @Override
  boolean equals(byte[] paramArrayOfByte, int paramInt1, int paramInt2) {
    throw new UnsupportedOperationException();
  }

  @Override
  boolean equals(Binary paramBinary) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compareTo(Binary paramBinary) {
    try {
      this.getBytes();
      paramBinary.getBytes();
    } catch (Throwable t ){
      throw new RuntimeException(t);
    }
    if(paramBinary instanceof ByteBufBinary){
      return buf.compareTo(((ByteBufBinary) paramBinary).buf);
    }else{
      byte[] b = paramBinary.getBytes();
      return compareTo(b, 0, b.length);
    }
  }

  @Override
  int compareTo(byte[] paramArrayOfByte, int paramInt1, int paramInt2) {
    ByteBuf b = Unpooled.wrappedBuffer(paramArrayOfByte, paramInt1, paramInt2);
    return buf.compareTo(b);
  }

  @Override
  public ByteBuffer toByteBuffer() {
    throw new UnsupportedOperationException();
  }
}