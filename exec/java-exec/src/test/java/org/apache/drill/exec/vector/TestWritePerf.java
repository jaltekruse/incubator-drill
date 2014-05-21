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
package org.apache.drill.exec.vector;

import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.SwappedByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: sphillips
 * Date: 5/20/14
 * Time: 2:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestWritePerf {

  static ByteBuffer buf;
  static byte[] bytes;
  BufferAllocator allocator = new TopLevelAllocator();

  @BeforeClass
  public static void initBuffer() {
    bytes = new byte[32*1024*4];
    new Random().nextBytes(bytes);
    buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
  }

  @Test
  public void test() throws Exception {
    IntVector intVector = new IntVector(MaterializedField.create("a", Types.required(MinorType.INT)), allocator);

    IntHolder holder = new IntHolder();

    for (int j = 0; j < 100; j++) {
      IntBuffer intBuffer = buf.asIntBuffer();
      intVector.allocateNew(32*1024);
      Stopwatch watch = new Stopwatch();
      watch.start();
      for (int i = 0; i < 32*1024; i++) {
        holder.value = intBuffer.get();
        intVector.getMutator().setSafe(i, holder);
      }
      long t = watch.elapsed(TimeUnit.NANOSECONDS);
      System.out.println(String.format("Took %d ns to read. %d ns / record", t, t /(32*1024)));
    }
  }

  @Test
  public void test4() throws Exception {
    NullableIntVector intVector = new NullableIntVector(MaterializedField.create("a", Types.required(MinorType.INT)), allocator);

    NullableIntHolder holder = new NullableIntHolder();

    for (int j = 0; j < 100; j++) {
      IntBuffer intBuffer = buf.asIntBuffer();
      intVector.allocateNew(32*1024);
      Stopwatch watch = new Stopwatch();
      watch.start();
      for (int i = 0; i < 32*1024; i++) {
        holder.value = intBuffer.get();
        intVector.getMutator().setSafe(i, holder);
      }
      long t = watch.elapsed(TimeUnit.NANOSECONDS);
      System.out.println(String.format("Took %d ns to read. %d ns / record", t, t /(32*1024)));
    }
  }

  @Test
  public void test3() throws Exception {
    IntVector intVector = new IntVector(MaterializedField.create("a", Types.required(MinorType.INT)), allocator);


    for (int j = 0; j < 100; j++) {
      IntBuffer intBuffer = buf.asIntBuffer();
      intVector.allocateNew(32*1024);
      Stopwatch watch = new Stopwatch();
      watch.start();
      ByteBuf byteBuf = new SwappedByteBuf(Unpooled.wrappedBuffer(buf));
      for (int i = 0; i < 32*1024; i++) {
        intVector.getData().writeBytes(byteBuf, 4);
      }
      long t = watch.elapsed(TimeUnit.NANOSECONDS);
      System.out.println(String.format("Took %d ns to read. %d ns / record", t, t /(32*1024)));
    }
  }

  @Test
  public void test2() throws Exception {
    IntVector intVector = new IntVector(MaterializedField.create("a", Types.required(MinorType.INT)), allocator);

    for (int j = 0; j < 100; j++) {
      IntBuffer intBuffer = buf.asIntBuffer();
      intVector.allocateNew(32*1024);
      Stopwatch watch = new Stopwatch();
      watch.start();
      intVector.getData().writeBytes(buf);
//      intVector.getData().writeBytes(bytes);
      long t = watch.elapsed(TimeUnit.NANOSECONDS);
      System.out.println(String.format("Took %d ns to read. %d ns / record", t, t /(32*1024)));
    }
  }
}
