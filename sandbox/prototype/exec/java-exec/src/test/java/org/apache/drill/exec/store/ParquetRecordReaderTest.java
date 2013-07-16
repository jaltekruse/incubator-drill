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
package org.apache.drill.exec.store;

import com.beust.jcommander.internal.Lists;
import mockit.Expectations;
import mockit.Injectable;
import org.apache.drill.common.exceptions.ExecutionSetupException;

import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.page.Page;
import parquet.column.page.PageReadStore;
import parquet.column.page.PageReader;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.PrintFooter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static parquet.column.Encoding.PLAIN;

public class ParquetRecordReaderTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorageEngineRegistry.class);

  private String getResource(String resourceName) {
    return "resource:" + resourceName;
  }

  class MockOutputMutator implements OutputMutator {
    List<Integer> removedFields = Lists.newArrayList();
    List<ValueVector.Base> addFields = Lists.newArrayList();

    @Override
    public void removeField(int fieldId) throws SchemaChangeException {
      removedFields.add(fieldId);
    }

    @Override
    public void addField(int fieldId, ValueVector.Base vector) throws SchemaChangeException {
      addFields.add(vector);
    }

    @Override
    public void setNewSchema() throws SchemaChangeException {
    }

    List<Integer> getRemovedFields() {
      return removedFields;
    }

    List<ValueVector.Base> getAddFields() {
      return addFields;
    }
  }

  @Test
  public void testBasicWriteRead() throws Exception {

    File testFile = new File("/tmp/testParquetFile_many_types").getAbsoluteFile();
    System.out.println(testFile.toPath().toString());
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    //"message m { required int32 integer; required int64 integer64; required boolean b; required float f; required double d;}"

    // format: type, field name, uncompressed size, value1, value2, value3
    Object[][] fields = {
        {"int32", "integer", 4, -100, 100, Integer.MAX_VALUE},
        {"int64", "bigInt", 8, -5000l, 5000l, Long.MAX_VALUE},
        {"boolean", "b", 1, true, false, true},
        {"float", "f", 4, 1.74f, Float.MAX_VALUE, Float.MIN_VALUE},
        {"double", "d", 8, 1.74d, Double.MAX_VALUE, Double.MIN_VALUE}
    };
    String messageSchema = "message m {";
    for (Object[] fieldInfo : fields) {
      messageSchema += " required " + fieldInfo[0] + " " + fieldInfo[1] + ";";
    }
    // remove the last semicolon, java really needs a join method for strings...
    // TODO - nvm apparently it requires a semicolon after every field decl, might want to file a bug
    //messageSchema = messageSchema.substring(0, messageSchema.length() - 1);
    messageSchema += "}";

    MessageType schema = MessageTypeParser.parseMessageType(messageSchema);

    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
    w.start();
    w.startBlock(1);
    for (Object[] fieldInfo : fields) {

      String[] path1 = {(String) fieldInfo[1]};
      ColumnDescriptor c1 = schema.getColumnDescription(path1);

      w.startColumn(c1, 300, codec);
      byte[] bytes = new byte[ (int) fieldInfo[2] * 3 * 100];
      for (int i = 0; i < 100; i++) {
        System.arraycopy(toByta(fieldInfo[3]), 0, bytes, ( i + 1 ) * 3 - 3, (int) fieldInfo[2]);
        System.arraycopy(toByta(fieldInfo[4]), 0, bytes, ( i + 1 ) * 3 - 2, (int) fieldInfo[2]);
        System.arraycopy(toByta(fieldInfo[5]), 0, bytes, ( i + 1 ) * 3 - 1, (int) fieldInfo[2]);
      }
      w.writeDataPage(300, bytes.length, BytesInput.from(bytes), PLAIN, PLAIN, PLAIN);
      w.writeDataPage(300, bytes.length, BytesInput.from(bytes), PLAIN, PLAIN, PLAIN);
      w.endColumn();
    }

    w.endBlock();
    w.end(new HashMap<String, String>());
  }

  @Test
  public void testBasicWrite() throws Exception {

    File testFile = new File("/tmp/testParquetFile").getAbsoluteFile();
    System.out.println(testFile.toPath().toString());
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    MessageType schema = MessageTypeParser.parseMessageType("message m { required int64 d; }");
    String[] path2 = { "d"};
    ColumnDescriptor c2 = schema.getColumnDescription(path2);

    byte[] bytes1 = { 0, 1, 2, 3};
    byte[] bytes2 = { 1, 2, 3, 4};
    byte[] bytes3 = { 2, 3, 4, 5};
    int numValues = 8;
    int datalength = 8;
    byte[] bytes4 = new byte[ numValues * datalength];
    for (int i = 0; i < bytes4.length; i++){
      bytes4[i] = (byte) i;
    }
    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
    w.start();
    w.startBlock(4);
    w.startColumn(c2, 8, codec);
    w.writeDataPage(8, bytes4.length, BytesInput.from(bytes4), PLAIN, PLAIN, PLAIN);
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<String, String>());
    PrintFooter.main(new String[] {path.toString()});
  }

  private <T> void assertField(ValueVector.Base valueVector, int index, SchemaDefProtos.MinorType expectedMinorType, T value, String name) {
    assertField(valueVector, index, expectedMinorType, value, name, 0);
  }

  private <T> void assertField(ValueVector.Base valueVector, int index, SchemaDefProtos.MinorType expectedMinorType, T value, String name, int parentFieldId) {
    UserBitShared.FieldMetadata metadata = valueVector.getMetadata();
    SchemaDefProtos.FieldDef def = metadata.getDef();
    assertEquals(expectedMinorType, def.getMajorType().getMinorType());
    assertEquals(name, def.getNameList().get(0).getName());
    assertEquals(parentFieldId, def.getParentId());

    if(expectedMinorType == SchemaDefProtos.MinorType.MAP) {
      return;
    }

    T val = (T) valueVector.getObject(index);
    if (val instanceof byte[]) {
      assertTrue(Arrays.equals((byte[]) value, (byte[]) val));
    } else {
      assertEquals(value, val);
    }
  }

  private class WrapAroundCounter{

    int maxVal;
    int val;
    public WrapAroundCounter(int maxVal){
      this.maxVal = maxVal;
    }

    public int increment(){
      val++;
      if (val > maxVal){
        val = 0;
      }
      return val;
    }

    public void reset(){
      val = 0;
    }

  }

  @Test
  public void parquetTest(@Injectable final FragmentContext context) throws IOException, ExecutionSetupException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new DirectBufferAllocator());
      }
    };

    File testFile = new File("/tmp/testParquetFile_many_types").getAbsoluteFile();
    System.out.println(testFile.toPath().toString());
    testFile.delete();

    Path path = new Path(testFile.toURI());
    Configuration configuration = new Configuration();

    //"message m { required int32 integer; required int64 integer64; required boolean b; required float f; required double d;}"

    // indices into the following array (to avoid indexing errors, and allow for future expansion)
    int schemaType = 0, fieldName = 1, bitLength = 2, numPages = 3, val1 = 4, val2 = 5, val3 = 6, minorType = 7;
    // format: type, field name, uncompressed size in bits, number of pages, value1, value2, value3
    Object[][] fields = {
        {"int32", "integer", 32, 8, -200, 100, Integer.MAX_VALUE, SchemaDefProtos.MinorType.INT},
        {"int64", "bigInt", 64, 4, -5000l, 5000l, Long.MAX_VALUE, SchemaDefProtos.MinorType.BIGINT},
        {"float", "f", 32, 8, 1.74f, Float.MAX_VALUE, Float.MIN_VALUE, SchemaDefProtos.MinorType.FLOAT4},
        {"double", "d", 64, 4, 100.45d, Double.MAX_VALUE, Double.MIN_VALUE, SchemaDefProtos.MinorType.FLOAT8},
        {"boolean", "b", 1, 2, false, false, true, SchemaDefProtos.MinorType.BOOLEAN}
    };
    String messageSchema = "message m {";
    for (Object[] fieldInfo : fields) {
      messageSchema += " required " + fieldInfo[schemaType] + " " + fieldInfo[fieldName] + ";";
    }
    // remove the last semicolon, java really needs a join method for strings...
    // TODO - nvm apparently it requires a semicolon after every field decl, might want to file a bug
    //messageSchema = messageSchema.substring(schemaType, messageSchema.length() - 1);
    messageSchema += "}";

    MessageType schema = MessageTypeParser.parseMessageType(messageSchema);

    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
    ParquetFileWriter w = new ParquetFileWriter(configuration, schema, path);
    w.start();
    w.startBlock(1);
    int numTotalVals = 18000;
    // { 00000001, 00000010, 00000100, 00001000, 00010000, ... }
    byte[] bitFields = { 1, 2, 4, 8, 16, 32, 64, -128};
    WrapAroundCounter booleanBitCounter = new WrapAroundCounter(7);
    int currentBooleanByte = 0;
    byte allBitsTrue = -1;
    byte allBitsFalse = 0;
    for (Object[] fieldInfo : fields) {

      String[] path1 = {(String) fieldInfo[fieldName]};
      ColumnDescriptor c1 = schema.getColumnDescription(path1);


      w.startColumn(c1, numTotalVals, codec);
      int valsPerPage = (int) Math.ceil(numTotalVals / (float)((int) fieldInfo[numPages]));
      byte[] bytes = new byte[ (int) Math.ceil(valsPerPage * (int) fieldInfo[bitLength] / 8.0) ];
      int bytesPerPage = (int) (valsPerPage * ((int)fieldInfo[bitLength] / 8.0));
      for (int i = 0; i < Math.ceil(valsPerPage / 3.0); i++) {
        System.out.print(i + ", " + (i % 25 == 0 ? "\n gen: " : ""));
        if (fieldInfo[4] instanceof Boolean){
          bytes[currentBooleanByte] |= bitFields[booleanBitCounter.val] & ((boolean) fieldInfo[val1] ? allBitsTrue : allBitsFalse);
          booleanBitCounter.increment();
          if (booleanBitCounter.val == 0) { currentBooleanByte++; }
          if (currentBooleanByte > bytesPerPage) break;
          bytes[currentBooleanByte] |= bitFields[booleanBitCounter.val] & ((boolean) fieldInfo[val2] ? allBitsTrue : allBitsFalse);
          booleanBitCounter.increment();
          if (booleanBitCounter.val == 0) { currentBooleanByte++; }
          if (currentBooleanByte > bytesPerPage) break;
          bytes[currentBooleanByte] |= bitFields[booleanBitCounter.val] & ((boolean) fieldInfo[val3] ? allBitsTrue : allBitsFalse);
          booleanBitCounter.increment();
          if (booleanBitCounter.val == 0) { currentBooleanByte++; }
          if (currentBooleanByte > bytesPerPage) break;
        }
        else{
          int j = (( i + 1 ) * 3 - 3) * ((int) fieldInfo[bitLength] / 8);
          System.arraycopy(toByta(fieldInfo[val1]), 0, bytes, (( i + 1 ) * 3 - 3) * ((int) fieldInfo[bitLength] / 8), (int) fieldInfo[bitLength] / 8);
          System.arraycopy(toByta(fieldInfo[val2]), 0, bytes, (( i + 1 ) * 3 - 2) * ((int) fieldInfo[bitLength] / 8), (int) fieldInfo[bitLength] / 8);
          System.arraycopy(toByta(fieldInfo[val3]), 0, bytes, (( i + 1 ) * 3 - 1) * ((int) fieldInfo[bitLength] / 8), (int) fieldInfo[bitLength] / 8);
        }
      }
      for (int i = 0; i < (int) fieldInfo[numPages]; i++){
        w.writeDataPage(numTotalVals / (int) fieldInfo[numPages] , bytes.length, BytesInput.from(bytes), PLAIN, PLAIN, PLAIN);
      }
      w.endColumn();
    }

    w.endBlock();
    w.end(new HashMap<String, String>());

    //File testFile = new File("exec/java-exec/src/test/resources/testParquetFile").getAbsoluteFile();
    testFile = new File("/tmp/testParquetFile_many_types").getAbsoluteFile();
    System.out.println(testFile.toPath().toString());
    //testFile.delete();

    path = new Path(testFile.toURI());
    configuration = new Configuration();

    ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration, path);

    ParquetFileReader parReader = new ParquetFileReader(configuration, path, Arrays.asList(
        readFooter.getBlocks().get(0)), readFooter.getFileMetaData().getSchema().getColumns());
    ParquetRecordReader pr = new ParquetRecordReader(context, parReader, readFooter);

    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector.Base> addFields = mutator.getAddFields();
    pr.setup(mutator);
    int batchCounter = 1;
    while(pr.next() > 0){
      int i = 0;
      for (ValueVector.Base vv : addFields) {
        System.out.println("\n" + (String) fields[i][fieldName]);
        for(int j = 0; j < vv.getRecordCount(); j++){
          if (j == 10863){
            Math.min(4,5);
            vv.data.writeByte(-2);
          }
          System.out.print(vv.getObject(j) + ", " + (j % 25 == 0 ? "\n batch:" + batchCounter + " v:" + j + " - ": ""));
          assertField(addFields.get(i), j, (SchemaDefProtos.MinorType) fields[i][minorType], fields[i][ val1 + j % 3], (String) fields[i][fieldName] + "/");
        }
        System.out.println("\n" + vv.getRecordCount());
        i++;
      }
      batchCounter++;
    }



    assertEquals(5, addFields.size());
  }

  public static byte[] toByta(Object data) {
    if ( data instanceof Integer) return toByta((int) data);
    else if ( data instanceof Double) return toByta((double) data);
    else if ( data instanceof Float) return toByta((float) data);
    else if ( data instanceof Boolean) return toByta((boolean) data);
    else if ( data instanceof Long) return toByta((long) data);
    else return null;
  }
  // found at http://www.daniweb.com/software-development/java/code/216874/primitive-types-as-byte-arrays
  /* ========================= */
  /* "primitive type --> byte[] data" Methods */
  /* ========================= */
  public static byte[] toByta(byte data) {
    return new byte[]{data};
  }

  public static byte[] toByta(byte[] data) {
    return data;
  }

  /* ========================= */
  public static byte[] toByta(short data) {
    return new byte[]{
        (byte) ((data >> 8) & 0xff),
        (byte) ((data >> 0) & 0xff),
    };
  }

  public static byte[] toByta(short[] data) {
    if (data == null) return null;
    // ----------
    byte[] byts = new byte[data.length * 2];
    for (int i = 0; i < data.length; i++)
      System.arraycopy(toByta(data[i]), 0, byts, i * 2, 2);
    return byts;
  }

  /* ========================= */
  public static byte[] toByta(char data) {
    return new byte[]{
        (byte) ((data >> 8) & 0xff),
        (byte) ((data >> 0) & 0xff),
    };
  }

  public static byte[] toByta(char[] data) {
    if (data == null) return null;
    // ----------
    byte[] byts = new byte[data.length * 2];
    for (int i = 0; i < data.length; i++)
      System.arraycopy(toByta(data[i]), 0, byts, i * 2, 2);
    return byts;
  }

  /* ========================= */
  public static byte[] toByta(int data) {
    return new byte[]{
        (byte) ((data >> 24) & 0xff),
        (byte) ((data >> 16) & 0xff),
        (byte) ((data >> 8) & 0xff),
        (byte) ((data >> 0) & 0xff),
    };
  }

  public static byte[] toByta(int[] data) {
    if (data == null) return null;
    // ----------
    byte[] byts = new byte[data.length * 4];
    for (int i = 0; i < data.length; i++)
      System.arraycopy(toByta(data[i]), 0, byts, i * 4, 4);
    return byts;
  }

  /* ========================= */
  public static byte[] toByta(long data) {
    return new byte[]{
        (byte) ((data >> 56) & 0xff),
        (byte) ((data >> 48) & 0xff),
        (byte) ((data >> 40) & 0xff),
        (byte) ((data >> 32) & 0xff),
        (byte) ((data >> 24) & 0xff),
        (byte) ((data >> 16) & 0xff),
        (byte) ((data >> 8) & 0xff),
        (byte) ((data >> 0) & 0xff),
    };
  }

  public static byte[] toByta(long[] data) {
    if (data == null) return null;
    // ----------
    byte[] byts = new byte[data.length * 8];
    for (int i = 0; i < data.length; i++)
      System.arraycopy(toByta(data[i]), 0, byts, i * 8, 8);
    return byts;
  }

  /* ========================= */
  public static byte[] toByta(float data) {
    return toByta(Float.floatToRawIntBits(data));
  }

  public static byte[] toByta(float[] data) {
    if (data == null) return null;
    // ----------
    byte[] byts = new byte[data.length * 4];
    for (int i = 0; i < data.length; i++)
      System.arraycopy(toByta(data[i]), 0, byts, i * 4, 4);
    return byts;
  }

  /* ========================= */
  public static byte[] toByta(double data) {
    return toByta(Double.doubleToRawLongBits(data));
  }

  public static byte[] toByta(double[] data) {
    if (data == null) return null;
    // ----------
    byte[] byts = new byte[data.length * 8];
    for (int i = 0; i < data.length; i++)
      System.arraycopy(toByta(data[i]), 0, byts, i * 8, 8);
    return byts;
  }

  /* ========================= */
  public static byte[] toByta(boolean data) {
    return new byte[]{(byte) (data ? 0x01 : 0x00)}; // bool -> {1 byte}
  }

  public static byte[] toByta(boolean[] data) {
    // Advanced Technique: The byte array containts information
    // about how many boolean values are involved, so the exact
    // array is returned when later decoded.
    // ----------
    if (data == null) return null;
    // ----------
    int len = data.length;
    byte[] lena = toByta(len); // int conversion; length array = lena
    byte[] byts = new byte[lena.length + (len / 8) + (len % 8 != 0 ? 1 : 0)];
    // (Above) length-array-length + sets-of-8-booleans +? byte-for-remainder
    System.arraycopy(lena, 0, byts, 0, lena.length);
    // ----------
    // (Below) algorithm by Matthew Cudmore: boolean[] -> bits -> byte[]
    for (int i = 0, j = lena.length, k = 7; i < data.length; i++) {
      byts[j] |= (data[i] ? 1 : 0) << k--;
      if (k < 0) {
        j++;
        k = 7;
      }
    }
    // ----------
    return byts;
  }

  // above utility methods found here:
  // http://www.daniweb.com/software-development/java/code/216874/primitive-types-as-byte-arrays

  private void validateFooters(final List<Footer> metadata) {
    logger.debug(metadata.toString());
    assertEquals(3, metadata.size());
    for (Footer footer : metadata) {
      final File file = new File(footer.getFile().toUri());
      assertTrue(file.getName(), file.getName().startsWith("part"));
      assertTrue(file.getPath(), file.exists());
      final ParquetMetadata parquetMetadata = footer.getParquetMetadata();
      assertEquals(2, parquetMetadata.getBlocks().size());
      final Map<String, String> keyValueMetaData = parquetMetadata.getFileMetaData().getKeyValueMetaData();
      assertEquals("bar", keyValueMetaData.get("foo"));
      assertEquals(footer.getFile().getName(), keyValueMetaData.get(footer.getFile().getName()));
    }
  }

  private void validateContains(MessageType schema, PageReadStore pages, String[] path, int values, BytesInput bytes)
      throws IOException {
    PageReader pageReader = pages.getPageReader(schema.getColumnDescription(path));
    Page page = pageReader.readPage();
    assertEquals(values, page.getValueCount());
    assertArrayEquals(bytes.toByteArray(), page.getBytes().toByteArray());
  }

}
