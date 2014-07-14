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
package parquet.hadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import parquet.bytes.BytesInput;
import parquet.hadoop.metadata.CompressionCodecName;

public class CodecFactoryExposer{

  private CodecFactory codecFactory;
  private final Map<String, CompressionCodec> codecByName = new HashMap<String, CompressionCodec>();
  private Configuration configuration;

  public CodecFactoryExposer(Configuration config){
    codecFactory = new CodecFactory(config);configuration = config;
  }

  public CodecFactory getCodecFactory() {
    return codecFactory;
  }

  public BytesInput decompress(BytesInput bytes, int uncompressedSize, CompressionCodecName codecName) throws IOException {
    return codecFactory.getDecompressor(codecName).decompress(bytes, uncompressedSize);
  }
  public BytesInput decompress(CompressionCodecName codecName,
                               ByteBuf compressedByteBuf,
                               ByteBuf uncompressedByteBuf,
                               int uncompressedSize) throws IOException {
    ByteBuffer inpBuffer=compressedByteBuf.nioBuffer();
    ByteBuffer outBuffer=uncompressedByteBuf.nioBuffer(0, uncompressedSize);
    //((DirectDecompressionCodec)codecFactory.getDecompressor(codecName)).createDirectDecompressor().decompress(inpBuffer, outBuffer);
    //TODO: The getCodec function below may return NULL if the file is uncompressed. The decompress path should be short circuited
    // anyway. Need to fix this code as well as the three places where this is called from PageReadStatus

    ((DirectDecompressionCodec)getCodec(codecName)).createDirectDecompressor().decompress(inpBuffer, outBuffer);
    return new HadoopByteBufBytesInput(outBuffer, 0, outBuffer.limit());
  }

  private CompressionCodec getCodec(CompressionCodecName codecName) {
    String codecClassName = codecName.getHadoopCompressionCodecClassName();
    if (codecClassName == null) {
      return null;
    }
    CompressionCodec codec = codecByName.get(codecClassName);
    if (codec != null) {
      return codec;
    }

    try {
      Class<?> codecClass = Class.forName(codecClassName);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);
      codecByName.put(codecClassName, codec);
      return codec;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
    }
  }

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
}
