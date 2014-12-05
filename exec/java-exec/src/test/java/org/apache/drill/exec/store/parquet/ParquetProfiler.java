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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.ClassPathFileSystem;
import org.apache.drill.exec.store.LocalSyncableFileSystem;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetRecordReader;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ParquetProfiler {


  private static final int MAX_RECORD_CNT = Character.MAX_VALUE;

  public static void main(String[] args) throws Exception{

    String file = "/tmp/orders_part-m-00001.parquet";
//    String file = "/tmp/parquet_with_nulls_should_sum_100000.parquet/part-m-00000.parquet";

    testParquetRead(file);
//    testRawTransfer();
    /*
    int numThreads = 5;
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new ReadThread();
    }
    for (Thread t : threads) {
      t.start();
    }
    */
  }

  public static class ReadThread extends Thread {

    public void run() {
      DecimalFormat formatter = new DecimalFormat("#,###.00");

      startTime = System.nanoTime();

//      testByteBufferTransfer();
      testByteArrayTransfer();

      System.out.println("Total read time:" + ((System.nanoTime() - startTime) / 1E9));
    }

  }

  // ============================================
  // fields to share between the next two methods
  // ============================================
  static int numberGigs = 3;
  static int iterations = 1024 * 1024 * numberGigs;
  static int len = 1024;
  static int transferLen = 1024;
  static TopLevelAllocator allocator = new TopLevelAllocator();
  static int transfers = len / transferLen;
  static long startTime;

  public static void testByteArrayTransfer() {

    byte[] src = new byte[len];
    byte[] dest = new byte[len];
    System.out.println("allocation time:" + ((System.nanoTime() - startTime) / 1E9));

    for (int j = 0; j < iterations; j++ ) {

      for (int i = 0; i < transfers; i++ ) {
//        for (int k = 0; k < transferLen; k++) {
//          dest[i] = src[i];
//        }
        System.arraycopy(src, i * transferLen, dest, i * transferLen, transferLen);
      }
    }
  }

  public static void testByteBufferTransfer() {

    DrillBuf src;
    DrillBuf dest;

    src = allocator.buffer(len);
    dest = allocator.buffer(len);
    System.out.println("allocation time:" + ((System.nanoTime() - startTime) / 1E9));
    byte[] temp = new byte[len];

    for (int j = 0; j < iterations; j++ ) {

//      src = allocator.buffer(len);
//      dest = allocator.buffer(len);

      for (int i = 0; i < transfers; i++ ) {
//        src.getBytes(i * transferLen, temp, i * transferLen, transferLen);
        dest.setBytes(i * transferLen, src, i * transferLen, transferLen);
      }
//      dest.setBytes(0, temp, 0, len);
      dest.setIndex(0,0);
//      src.clear();
//      dest.clear();
    }

  }

  public static void testParquetRead(String file) throws IOException, ExecutionSetupException {

    Configuration fsConf = new Configuration();
    fsConf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    fsConf.set("fs.classpath.impl", ClassPathFileSystem.class.getName());
    fsConf.set("fs.drill-local.impl", LocalSyncableFileSystem.class.getName());
    FileSystem fs = FileSystem.get(fsConf);
    CodecFactoryExposer codecFactoryExposer = new CodecFactoryExposer(fs.getConf());
    ParquetMetadata footer = ParquetFileReader.readFooter(fs.getConf(), new Path(file));
    List<ParquetRecordReader> readers = new ArrayList();
    long startTime = System.nanoTime();
    long totalTime = 0;
    DecimalFormat formatter = new DecimalFormat("#,###.00");
    List<SchemaPath> columns = Lists.newArrayList();
    columns.add(AbstractRecordReader.STAR_COLUMN);
//    columns.add(new SchemaPath(new PathSegment.NameSegment("O_ORDERKEY")));
//    columns.add(new SchemaPath(new PathSegment.NameSegment("O_CUSTKEY")));
//    columns.add(new SchemaPath(new PathSegment.NameSegment("O_TOTALPRICE")));
//    columns.add(new SchemaPath(new PathSegment.NameSegment("O_SHIPPRIORITY")));
    TopLevelAllocator allocator = new TopLevelAllocator();
    for (int i = 0; i < 3; i++ ) {
      readers.add(new ParquetRecordReader(null, // frag context
          file, // path
          0, // row group number
          fs, //
          codecFactoryExposer, //
          footer, //
          columns)); // columns to read, null or empty list means read all columns
      readers.get(i).setAllocator(allocator);
    }
    Mutator mutator = new Mutator(new TopLevelAllocator());
    long totalRecordsRead = 0;
    for ( ParquetRecordReader rr : readers){
      rr.setup(mutator);

      int recordsRead = 0;
      mutator.allocate(4096);
      startTime = System.nanoTime();
      do {
        mutator.allocate(4096);
//        recordsRead = rr.next();
        // read all of the data, but do not transfer it into vectors
        recordsRead = (int) rr.next();
        totalRecordsRead += recordsRead;
//        System.out.println(recordsRead);
      } while (recordsRead > 0);
      totalTime += (System.nanoTime() - startTime) / 1E9;
      rr.cleanup();
      System.out.println("Total records read: " + formatter.format(totalRecordsRead));
    }

    System.out.println("Total records read: " + formatter.format(totalRecordsRead));
    System.out.println("Total read time:" + ((System.nanoTime() - startTime) / 1E9));
//    System.out.println("Total MB read: " + ColumnDataReader.totalReadLength / ( 1024 * 1024));
//    System.out.println("Total read and s:" + ((System.nanoTime() - startTime) / 1E9));

  }

  private static class Mutator implements OutputMutator {

    boolean schemaChange = true;
    private final VectorContainer container = new VectorContainer();
    private final Map<MaterializedField.Key, ValueVector> fieldVectorMap = Maps.newHashMap();
    private TopLevelAllocator allocator;

    public Mutator(TopLevelAllocator allocator) {
      this.allocator = allocator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends ValueVector> T addField(MaterializedField field, Class<T> clazz) throws SchemaChangeException {
      // Check if the field exists
      ValueVector v = fieldVectorMap.get(field.key());

      if (v == null || v.getClass() != clazz) {
        // Field does not exist add it to the map and the output container
        v = TypeHelper.getNewVector(field, allocator);
        if(!clazz.isAssignableFrom(v.getClass())) {
          throw new SchemaChangeException(String.format("The class that was provided %s does not correspond to the expected vector type of %s.", clazz.getSimpleName(), v.getClass().getSimpleName()));
        }
        container.add(v);
        fieldVectorMap.put(field.key(), v);

        // Adding new vectors to the container mark that the schema has changed
        schemaChange = true;
      }

      return (T) v;
    }

    @Override
    public void addFields(List<ValueVector> vvList) {
      for (ValueVector v : vvList) {
        fieldVectorMap.put(v.getField().key(), v);
        container.add(v);
      }
      schemaChange = true;
    }

    @Override
    public DrillBuf getManagedBuffer() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void allocate(int recordCount) {
      for (ValueVector v : fieldVectorMap.values()) {
        AllocationHelper.allocate(v, recordCount, 50, 10);
      }
    }

    @Override
    public boolean isNewSchema() {
      if (schemaChange == true) {
        schemaChange = false;
        return true;
      }
      return false;
    }
  }
}
