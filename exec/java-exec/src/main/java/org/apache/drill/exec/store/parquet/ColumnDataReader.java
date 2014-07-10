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

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.bytes.BytesInput;
import parquet.format.PageHeader;
import parquet.format.Util;
import parquet.hadoop.util.CompatibilityUtil;

//import static parquet.bytes.BytesInput.ByteBufferBytesInput;

public class ColumnDataReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnDataReader.class);
  
  private final long endPosition;
  public final FSDataInputStream input;
  
  public ColumnDataReader(FSDataInputStream input, long start, long length) throws IOException{
    this.input = input;
    this.input.seek(start);
    this.endPosition = start + length;
  }
  
  public PageHeader readPageHeader() throws IOException{
    return Util.readPageHeader(input);
  }

/*  Based on implementation in Parquet's CompatibilityUtil
  public static ByteBuf getBuf(FSDataInputStream f, int maxSize) throws IOException {
    Class<?>[] ZCopyArgs = {ByteBuffer.class};
    int res=0;
    // TODO: ALLOCATE the buffer ...
    ByteBuf readBuf;
    try {
      //Use Zero Copy API if available
      f.getClass().getMethod("read", ZCopyArgs);
      ByteBuffer buffer=readBuf.nioBuffer();
      buffer.limit(maxSize);
      res = f.read(buffer);
    } catch (NoSuchMethodException e) {
      byte[] buf = new byte[maxSize];
      f.readFully(buf);
      readBuf = Unpooled.wrappedBuffer(ByteBuffer.wrap(buf));
    }
    if (res == 0) {
      throw new EOFException("Null ByteBuffer returned");
    }
    return readBuf;
  }
*/

  public BytesInput getPageAsBytesInput(int pageLength) throws IOException{
    byte[] b = new byte[pageLength];
    input.read(b);
    return new HadoopBytesInput(b);
  }

  public ByteBuf getPageAsBytesBuf(ByteBuf byteBuf, int pageLength) throws IOException{
    ByteBuffer directBuffer=byteBuf.nioBuffer();
    CompatibilityUtil.getBuf(input, directBuffer, pageLength);
    //BytesInput bb= new HadoopByteBufBytesInput(directBuffer, 0, pageLength);
    return byteBuf;
  }

  public void clear(){
    try{
      input.close();
    }catch(IOException ex){
      logger.warn("Error while closing input stream.", ex);
    }
  }

  public boolean hasRemainder() throws IOException{
    return input.getPos() < endPosition;
  }
  
  public class HadoopBytesInput extends BytesInput{

    private final byte[] pageBytes;
    
    public HadoopBytesInput(byte[] pageBytes) {
      super();
      this.pageBytes = pageBytes;
    }

    @Override
    public byte[] toByteArray() throws IOException {
      return pageBytes;
    }

    @Override
    public long size() {
      return pageBytes.length;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      out.write(pageBytes);
    }
    
  }

  /*
  public class HadoopByteBufBytesInput extends BytesInput{

    private final ByteBuffer byteBuf;
    private final int length;
    private final int offset;

    private HadoopByteBufBytesInput(ByteBuffer byteBuf, int offset, int length) {
      super();
      this.byteBuf = byteBuf;
      this.offset = offset;
      this.length = length;
    }

    public void writeAllTo(OutputStream out) throws IOException {
      final WritableByteChannel outputChannel = Channels.newChannel(out);
      byteBuf.position(offset);
      ByteBuffer tempBuf = byteBuf.slice();
      tempBuf.limit(length);
      outputChannel.write(tempBuf);
    }

    public ByteBuffer toByteBuffer() throws IOException {
      byteBuf.position(offset);
      ByteBuffer buf = byteBuf.slice();
      buf.limit(length);
      return buf;
    }

    public long size() {
      return length;
    }
  }
  */
}
