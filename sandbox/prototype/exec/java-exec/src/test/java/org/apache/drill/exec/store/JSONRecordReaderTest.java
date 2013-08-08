package org.apache.drill.exec.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import mockit.Expectations;
import mockit.Injectable;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;


public class JSONRecordReaderTest {
  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private String getResource(String resourceName) {
    return "resource:" + resourceName;
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
    public void removeAllFields() {
      addFields.clear();
    }

    @Override
    public void setNewSchema() throws SchemaChangeException {
    }

    @Override
    public boolean containsField(MaterializedField field) {
      throw new UnsupportedOperationException();
    }

    List<MaterializedField> getRemovedFields() {
      return removedFields;
    }

    List<ValueVector> getAddFields() {
      return addFields;
    }
  }

  private <T> void assertField(ValueVector valueVector, int index, MinorType expectedMinorType, T value, String name) {
    UserBitShared.FieldMetadata metadata = valueVector.getMetadata();
    SchemaDefProtos.FieldDef def = metadata.getDef();
    assertEquals(expectedMinorType, def.getMajorType().getMinorType());
    String[] parts = name.split("\\.");
    assertEquals(parts.length, def.getNameList().size());
    for(int i = 0; i < parts.length; ++i) {
      assertEquals(parts[i], def.getName(i).getName());
    }

    if (expectedMinorType == MinorType.MAP) {
      return;
    }

    T val = (T) valueVector.getAccessor().getObject(index);
    if (val instanceof byte[]) {
      assertTrue(Arrays.equals((byte[]) value, (byte[]) val));
    } else {
      assertEquals(value, val);
    }
  }

  @Test
  public void testSameSchemaInSameBatch(@Injectable final FragmentContext context) throws IOException,
      ExecutionSetupException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new DirectBufferAllocator());
      }
    };
    JSONRecordReader jr = new JSONRecordReader(context, getResource("scan_json_test_1.json"));

    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();
    jr.setup(mutator);
    assertEquals(2, jr.next());
    assertEquals(3, addFields.size());
    assertField(addFields.get(0), 0, MinorType.INT, 123, "test");
    assertField(addFields.get(1), 0, MinorType.BIT, true, "b");
    assertField(addFields.get(2), 0, MinorType.VARCHAR, "hi!".getBytes(UTF_8), "c");
    assertField(addFields.get(0), 1, MinorType.INT, 1234, "test");
    assertField(addFields.get(1), 1, MinorType.BIT, false, "b");
    assertField(addFields.get(2), 1, MinorType.VARCHAR, "drill!".getBytes(UTF_8), "c");

    assertEquals(0, jr.next());
    assertTrue(mutator.getRemovedFields().isEmpty());
  }

  @Test
  public void testChangedSchemaInSameBatch(@Injectable final FragmentContext context) throws IOException,
      ExecutionSetupException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new DirectBufferAllocator());
      }
    };

    JSONRecordReader jr = new JSONRecordReader(context, getResource("scan_json_test_2.json"));
    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();

    jr.setup(mutator);
    assertEquals(3, jr.next());
    assertEquals(7, addFields.size());
    assertField(addFields.get(0), 0, MinorType.INT, 123, "test");
    assertField(addFields.get(1), 0, MinorType.INT, 1, "b");
    assertField(addFields.get(2), 0, MinorType.FLOAT4, (float) 2.15, "c");
    assertField(addFields.get(3), 0, MinorType.BIT, true, "bool");
    assertField(addFields.get(4), 0, MinorType.VARCHAR, "test1".getBytes(UTF_8), "str1");

    assertField(addFields.get(0), 1, MinorType.INT, 1234, "test");
    assertField(addFields.get(1), 1, MinorType.INT, 3, "b");
    assertField(addFields.get(3), 1, MinorType.BIT, false, "bool");
    assertField(addFields.get(4), 1, MinorType.VARCHAR, "test2".getBytes(UTF_8), "str1");
    assertField(addFields.get(5), 1, MinorType.INT, 4, "d");

    assertField(addFields.get(0), 2, MinorType.INT, 12345, "test");
    assertField(addFields.get(2), 2, MinorType.FLOAT4, (float) 5.16, "c");
    assertField(addFields.get(3), 2, MinorType.BIT, true, "bool");
    assertField(addFields.get(5), 2, MinorType.INT, 6, "d");
    assertField(addFields.get(6), 2, MinorType.VARCHAR, "test3".getBytes(UTF_8), "str2");
    assertTrue(mutator.getRemovedFields().isEmpty());
    assertEquals(0, jr.next());
  }

  @Test
  public void testChangedSchemaInTwoBatches(@Injectable final FragmentContext context) throws IOException,
      ExecutionSetupException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new DirectBufferAllocator());
      }
    };

    JSONRecordReader jr = new JSONRecordReader(context, getResource("scan_json_test_2.json"), 64); // batch only fits 1
                                                                                                   // int
    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();
    List<MaterializedField> removedFields = mutator.getRemovedFields();

    jr.setup(mutator);
    assertEquals(1, jr.next());
    assertEquals(5, addFields.size());
    assertField(addFields.get(0), 0, MinorType.INT, 123, "test");
    assertField(addFields.get(1), 0, MinorType.INT, 1, "b");
    assertField(addFields.get(2), 0, MinorType.FLOAT4, (float) 2.15, "c");
    assertField(addFields.get(3), 0, MinorType.BIT, true, "bool");
    assertField(addFields.get(4), 0, MinorType.VARCHAR, "test1".getBytes(UTF_8), "str1");
    assertTrue(removedFields.isEmpty());
    assertEquals(1, jr.next());
    assertEquals(6, addFields.size());
    assertField(addFields.get(0), 0, MinorType.INT, 1234, "test");
    assertField(addFields.get(1), 0, MinorType.INT, 3, "b");
    assertField(addFields.get(3), 0, MinorType.BIT, false, "bool");
    assertField(addFields.get(4), 0, MinorType.VARCHAR, "test2".getBytes(UTF_8), "str1");
    assertField(addFields.get(5), 0, MinorType.INT, 4, "d");
    assertEquals(1, removedFields.size());
    assertEquals("c", removedFields.get(0).getName());
    removedFields.clear();
    assertEquals(1, jr.next());
    assertEquals(8, addFields.size()); // The reappearing of field 'c' is also included
    assertField(addFields.get(0), 0, MinorType.INT, 12345, "test");
    assertField(addFields.get(3), 0, MinorType.BIT, true, "bool");
    assertField(addFields.get(5), 0, MinorType.INT, 6, "d");
    assertField(addFields.get(6), 0, MinorType.FLOAT4, (float) 5.16, "c");
    assertField(addFields.get(7), 0, MinorType.VARCHAR, "test3".getBytes(UTF_8), "str2");
    assertEquals(2, removedFields.size());
    Iterables.find(removedFields, new Predicate<MaterializedField>() {
      @Override
      public boolean apply(MaterializedField materializedField) {
        return materializedField.getName().equals("str1");
      }
    });
    Iterables.find(removedFields, new Predicate<MaterializedField>() {
      @Override
      public boolean apply(MaterializedField materializedField) {
        return materializedField.getName().equals("b");
      }
    });
    assertEquals(0, jr.next());
  }

  @Test
  public void testNestedFieldInSameBatch(@Injectable final FragmentContext context) throws ExecutionSetupException {
    new Expectations() {
      {
        context.getAllocator();
        returns(new DirectBufferAllocator());
      }
    };

    JSONRecordReader jr = new JSONRecordReader(context, getResource("scan_json_test_3.json"));

    MockOutputMutator mutator = new MockOutputMutator();
    List<ValueVector> addFields = mutator.getAddFields();
    jr.setup(mutator);
    assertEquals(2, jr.next());
    assertEquals(3, addFields.size());
    assertField(addFields.get(0), 0, MinorType.INT, 123, "test");
    assertField(addFields.get(1), 0, MinorType.VARCHAR, "test".getBytes(UTF_8), "a.b");
    assertField(addFields.get(2), 0, MinorType.BIT, true, "a.a.d");
    assertField(addFields.get(0), 1, MinorType.INT, 1234, "test");
    assertField(addFields.get(1), 1, MinorType.VARCHAR, "test2".getBytes(UTF_8), "a.b");
    assertField(addFields.get(2), 1, MinorType.BIT, false, "a.a.d");

    assertEquals(0, jr.next());
    assertTrue(mutator.getRemovedFields().isEmpty());
  }
}
