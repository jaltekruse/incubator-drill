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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.ValueVector;
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

  private boolean VERBOSE_DEBUG = false;

  private String getResource(String resourceName) {
    return "resource:" + resourceName;
  }


  @Test
  public void parquetTest(@Injectable final FragmentContext context) throws Exception {
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

    byte[] varLen1 = {50, 51, 52, 53, 54, 55, 56};
    byte[] varLen2 = {15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    byte[] varLen3 = {100, 99, 98};
    // indices into the following array (to avoid indexing errors, and allow for future expansion)
    int schemaType = 0, fieldName = 1, bitLength = 2, numPages = 3, val1 = 4, val2 = 5, val3 = 6, minorType = 7;
    // format: type, field name, uncompressed size in bits, number of pages, value1, value2, value3
    Object[][] fields = {
        {"int32", "integer", 32, 8, -200, 100, Integer.MAX_VALUE, TypeProtos.MinorType.INT},
        {"int64", "bigInt", 64, 4, -5000l, 5000l, Long.MAX_VALUE, TypeProtos.MinorType.BIGINT},
        {"float", "f", 32, 8, 1.74f, Float.MAX_VALUE, Float.MIN_VALUE, TypeProtos.MinorType.FLOAT4},
        {"double", "d", 64, 4, 100.45d, Double.MAX_VALUE, Double.MIN_VALUE, TypeProtos.MinorType.FLOAT8},
        {"boolean", "b", 1, 2, false, false, true, TypeProtos.MinorType.BOOLEAN}
//        {"binary", "bin", -1, 2, varLen1, varLen2, varLen3, TypeProtos.MinorType.VARBINARY4},
//        {"binary", "bin2", -1, 4, varLen1, varLen2, varLen3, TypeProtos.MinorType.VARBINARY4}
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
    int numTotalVals = 30000;
    // { 00000001, 00000010, 00000100, 00001000, 00010000, ... }
    byte[] bitFields = {1, 2, 4, 8, 16, 32, 64, -128};
    WrapAroundCounter booleanBitCounter = new WrapAroundCounter(7);
    int currentBooleanByte = 0;
    byte allBitsTrue = -1;
    byte allBitsFalse = 0;
    for (Object[] fieldInfo : fields) {

      String[] path1 = {(String) fieldInfo[fieldName]};
      ColumnDescriptor c1 = schema.getColumnDescription(path1);

      w.startColumn(c1, numTotalVals, codec);
      int valsPerPage = (int) Math.ceil(numTotalVals / (float) ((int) fieldInfo[numPages]));
      byte[] bytes;
      if ((int) fieldInfo[bitLength] > 0) {
        bytes = new byte[(int) Math.ceil(valsPerPage * (int) fieldInfo[bitLength] / 8.0)];
      } else {
        // the twelve at the end is to account for storing a 4 byte length with each value
        int totalValLength = ((byte[]) fieldInfo[val1]).length + ((byte[]) fieldInfo[val2]).length + ((byte[]) fieldInfo[val3]).length + 12;
        bytes = new byte[(int) Math.ceil(valsPerPage / 3 * totalValLength)];
      }
      int bytesPerPage = (int) (valsPerPage * ((int) fieldInfo[bitLength] / 8.0));
      int valsWritten = 0;
      int bytesWritten = 0;
      for (int z = 0; z < (int) fieldInfo[numPages]; z++) {
        bytesWritten = 0;
        valsWritten = 0;
          for (int i = 0; i < valsPerPage; i++) {
            //System.out.print(i + ", " + (i % 25 == 0 ? "\n gen " + fieldInfo[fieldName] + ": " : ""));
            if (fieldInfo[val1] instanceof Boolean) {

              bytes[currentBooleanByte] |= bitFields[booleanBitCounter.val] & ((boolean) fieldInfo[val1 + valsWritten % 3]
                  ? allBitsTrue : allBitsFalse);
              booleanBitCounter.increment();
              if (booleanBitCounter.val == 0) {
                currentBooleanByte++;
              }
              valsWritten++;
              if (currentBooleanByte > bytesPerPage) break;
            } else {
              if (fieldInfo[val1 + valsWritten % 3] instanceof byte[]){
                System.arraycopy(toByta(Integer.reverseBytes(((byte[])fieldInfo[val1 + valsWritten % 3]).length)),
                    0, bytes, bytesWritten, 4);
                System.arraycopy(fieldInfo[val1 + valsWritten % 3],
                    0, bytes, bytesWritten + 4, ((byte[])fieldInfo[val1 + valsWritten % 3]).length);
                bytesWritten += ((byte[])fieldInfo[val1 + valsWritten % 3]).length + 4;
              }
              else{
                System.arraycopy( toByta(fieldInfo[val1 + valsWritten % 3]),
                    0, bytes, i * ((int) fieldInfo[bitLength] / 8), (int) fieldInfo[bitLength] / 8);
              }

              valsWritten++;
            }

        }
        w.writeDataPage(numTotalVals / (int) fieldInfo[numPages], bytes.length, BytesInput.from(bytes), PLAIN, PLAIN, PLAIN);
        currentBooleanByte = 0;
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
    List<ValueVector> addFields = mutator.getAddFields();
    pr.setup(mutator);
    HashMap<MaterializedField, Integer> valuesChecked = new HashMap();
    for (ValueVector vv : addFields) {
      valuesChecked.put(vv.getField(), 0);
    }
    int batchCounter = 1;
    int columnValCounter = 0;
    while (pr.next() > 0) {
      int i = 0;
      for (ValueVector vv : addFields) {
        if (VERBOSE_DEBUG){
          System.out.println("\n" + (String) fields[i][fieldName]);
        }
        columnValCounter = valuesChecked.get(vv.getField());
        for (int j = 0; j < ((BaseDataValueVector)vv).getValueCount(); j++) {
          if (VERBOSE_DEBUG){
            System.out.print(vv.getAccessor().getObject(j) + ", " + (j % 25 == 0 ? "\n batch:" + batchCounter + " v:" + j + " - " : ""));
          }
          assertField(addFields.get(i), j, (TypeProtos.MinorType) fields[i][minorType],
              fields[i][val1 + columnValCounter % 3], (String) fields[i][fieldName] + "/");
          columnValCounter++;
        }
        if (VERBOSE_DEBUG){
          System.out.println("\n" + ((BaseDataValueVector)vv).getValueCount());
        }
        valuesChecked.remove(vv.getField());
        valuesChecked.put(vv.getField(), columnValCounter);
        i++;
      }
      batchCounter++;
    }
    //assertEquals(5, addFields.size());
  }

  class MockOutputMutator implements OutputMutator {
    List<MaterializedField> removedFields = Lists.newArrayList();
    List<ValueVector> addFields = Lists.newArrayList();

    @Override
    public void removeField(MaterializedField field) throws SchemaChangeException {
      removedFields.add(field);
    }

    @Override
    public void addField(ValueVector vector) throws SchemaChangeException {
      addFields.add(vector);
    }

    @Override
    public void setNewSchema() throws SchemaChangeException {
    }

    List<MaterializedField> getRemovedFields() {
      return removedFields;
    }

    List<ValueVector> getAddFields() {
      return addFields;
    }
  }

  private <T> void assertField(ValueVector valueVector, int index, TypeProtos.MinorType expectedMinorType, Object value, String name) {
    assertField(valueVector, index, expectedMinorType, value, name, 0);
  }

  private <T> void assertField(ValueVector valueVector, int index, TypeProtos.MinorType expectedMinorType, T value, String name, int parentFieldId) {
//    UserBitShared.FieldMetadata metadata = valueVector.getMetadata();
//    SchemaDefProtos.FieldDef def = metadata.getDef();
//    assertEquals(expectedMinorType, def.getMajorType().getMinorType());
//    assertEquals(name, def.getNameList().get(0).getName());
//    assertEquals(parentFieldId, def.getParentId());

    if (expectedMinorType == TypeProtos.MinorType.MAP) {
      return;
    }

    T val = (T) valueVector.getAccessor().getObject(index);
    if (val instanceof byte[]) {
        assertTrue(Arrays.equals((byte[]) value, (byte[]) val));
    } else {
      assertEquals(value, val);
    }
  }

  private class WrapAroundCounter {

    int maxVal;
    int val;

    public WrapAroundCounter(int maxVal) {
      this.maxVal = maxVal;
    }

    public int increment() {
      val++;
      if (val > maxVal) {
        val = 0;
      }
      return val;
    }

    public void reset() {
      val = 0;
    }

  }

  public static byte[] toByta(Object data) throws Exception {
    if (data instanceof Integer) return toByta((int) data);
    else if (data instanceof Double) return toByta((double) data);
    else if (data instanceof Float) return toByta((float) data);
    else if (data instanceof Boolean) return toByta((boolean) data);
    else if (data instanceof Long) return toByta((long) data);
    else throw new Exception("Cannot convert that type to a byte array.");
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
