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
package org.apache.drill.exec.store.hive;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableSmallIntVector;
import org.apache.drill.exec.vector.NullableTinyIntVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.SmallIntVector;
import org.apache.drill.exec.vector.TinyIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.collect.Lists;

public class HiveRecordReader implements RecordReader {

  protected Table table;
  protected Partition partition;
  protected InputSplit inputSplit;
  protected FragmentContext context;
  protected List<SchemaPath> columns;
  protected List<String> columnNames;
  protected List<String> partitionNames = Lists.newArrayList();
  protected List<String> selectedPartitionNames = Lists.newArrayList();
  protected List<String> selectedPartitionTypes = Lists.newArrayList();
  protected List<String> tableColumns;
  protected SerDe serde;
  protected StructObjectInspector sInspector;
  protected List<PrimitiveObjectInspector> fieldInspectors = Lists.newArrayList();
  protected List<PrimitiveCategory> primitiveCategories = Lists.newArrayList();
  protected Object key, value;
  protected org.apache.hadoop.mapred.RecordReader reader;
  protected List<ValueVector> vectors = Lists.newArrayList();
  protected List<ValueVector> pVectors = Lists.newArrayList();
  protected Object redoRecord;
  List<Object> partitionValues = Lists.newArrayList();
  protected boolean empty;

  protected static final int TARGET_RECORD_COUNT = 4000;

  public HiveRecordReader(Table table, Partition partition, InputSplit inputSplit, List<SchemaPath> columns, FragmentContext context) throws ExecutionSetupException {
    this.table = table;
    this.partition = partition;
    this.inputSplit = inputSplit;
    this.context = context;
    this.columns = columns;
    this.empty = (inputSplit == null && partition == null);
    init();
  }

  private void init() throws ExecutionSetupException {
    Properties properties;
    JobConf job = new JobConf();
    if (partition != null) {
      properties = MetaStoreUtils.getPartitionMetadata(partition, table);
    } else {
      properties = MetaStoreUtils.getTableMetadata(table);
    }
    for (Object obj : properties.keySet()) {
      job.set((String) obj, (String) properties.get(obj));
    }
    InputFormat format;
    String sLib = (partition == null) ? table.getSd().getSerdeInfo().getSerializationLib() : partition.getSd().getSerdeInfo().getSerializationLib();
    String inputFormatName = (partition == null) ? table.getSd().getInputFormat() : partition.getSd().getInputFormat();
    try {
      format = (InputFormat) Class.forName(inputFormatName).getConstructor().newInstance();
      Class c = Class.forName(sLib);
      serde = (SerDe) c.getConstructor().newInstance();
      serde.initialize(job, properties);
    } catch (ReflectiveOperationException | SerDeException e) {
      throw new ExecutionSetupException("Unable to instantiate InputFormat", e);
    }
    job.setInputFormat(format.getClass());

    List<FieldSchema> partitionKeys = table.getPartitionKeys();
    for (FieldSchema field : partitionKeys) {
      partitionNames.add(field.getName());
    }

    try {
      ObjectInspector oi = serde.getObjectInspector();
      if (oi.getCategory() != ObjectInspector.Category.STRUCT) {
        throw new UnsupportedOperationException(String.format("%s category not supported", oi.getCategory()));
      }
      sInspector = (StructObjectInspector) oi;
      StructTypeInfo sTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(sInspector);
      if (columns == null) {
        columnNames = sTypeInfo.getAllStructFieldNames();
        tableColumns = columnNames;
      } else {
        tableColumns = sTypeInfo.getAllStructFieldNames();
        List<Integer> columnIds = Lists.newArrayList();
        columnNames = Lists.newArrayList();
        for (SchemaPath field : columns) {
          String columnName = field.getRootSegment().getPath(); //TODO?
          if (!tableColumns.contains(columnName)) {
            if (partitionNames.contains(columnName)) {
              selectedPartitionNames.add(columnName);
            } else {
              throw new ExecutionSetupException(String.format("Column %s does not exist", columnName));
            }
          } else {
            columnIds.add(tableColumns.indexOf(columnName));
            columnNames.add(columnName);
          }
        }
        ColumnProjectionUtils.appendReadColumnIDs(job, columnIds);
        ColumnProjectionUtils.appendReadColumnNames(job, columnNames);
      }
      for (String columnName : columnNames) {
        ObjectInspector poi = sInspector.getStructFieldRef(columnName).getFieldObjectInspector();
        if(poi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
          throw new UnsupportedOperationException(String.format("%s type not supported", poi.getCategory()));
        }
        PrimitiveObjectInspector pInspector = (PrimitiveObjectInspector) poi;
        fieldInspectors.add(pInspector);
        primitiveCategories.add(pInspector.getPrimitiveCategory());
      }

      if (columns == null) {
        selectedPartitionNames = partitionNames;
      }

      for (int i = 0; i < table.getPartitionKeys().size(); i++) {
        FieldSchema field = table.getPartitionKeys().get(i);
        if (selectedPartitionNames.contains(field.getName())) {
          selectedPartitionTypes.add(field.getType());
          if (partition != null) {
            partitionValues.add(convertPartitionType(field.getType(), partition.getValues().get(i)));
          }
        }
      }
    } catch (SerDeException e) {
      throw new ExecutionSetupException(e);
    }

    if (!empty) {
      try {
        reader = format.getRecordReader(inputSplit, job, Reporter.NULL);
      } catch (IOException e) {
        throw new ExecutionSetupException("Failed to get Recordreader", e);
      }
      key = reader.createKey();
      value = reader.createValue();
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      for (int i = 0; i < columnNames.size(); i++) {
        PrimitiveCategory pCat = primitiveCategories.get(i);
        MajorType type = getMajorType(pCat);
        MaterializedField field = MaterializedField.create(SchemaPath.getSimplePath(columnNames.get(i)), type);
        ValueVector vv = output.addField(field, (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode()));
        vectors.add(vv);
      }
      for (int i = 0; i < selectedPartitionNames.size(); i++) {
        String type = selectedPartitionTypes.get(i);
        MaterializedField field = MaterializedField.create(SchemaPath.getSimplePath(selectedPartitionNames.get(i)), Types.getMajorTypeFromName(type));
        Class vvClass = TypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getDataMode());
        pVectors.add(output.addField(field, vvClass));
      }
    } catch(SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  protected void populatePartitionVectors(int recordCount) {
    for (int i = 0; i < pVectors.size(); i++) {
      int size = 50;
      ValueVector vector = pVectors.get(i);
      Object val = partitionValues.get(i);
      if (selectedPartitionTypes.get(i).equals("string") || selectedPartitionTypes.get(i).equals("binary")) {
        size = ((byte[]) partitionValues.get(i)).length;
      }
      VectorAllocator.getAllocator(vector, size).alloc(recordCount);
      switch(selectedPartitionTypes.get(i)) {
        case "boolean": {
          BitVector v = (BitVector) vector;
          Boolean value = (Boolean) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().set(j, value ? 1 : 0);
          }
          break;
        }
        case "tinyint": {
          TinyIntVector v = (TinyIntVector) vector;
          byte value = (byte) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case "double": {
          Float8Vector v = (Float8Vector) vector;
          double value = (double) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case "float": {
          Float4Vector v = (Float4Vector) vector;
          float value = (float) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case "int": {
          IntVector v = (IntVector) vector;
          int value = (int) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case "bigint": {
          BigIntVector v = (BigIntVector) vector;
          long value = (long) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case "smallint": {
          SmallIntVector v = (SmallIntVector) vector;
          short value = (short) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case "string": {
          VarCharVector v = (VarCharVector) vector;
          byte[] value = (byte[]) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case "timestamp": {
          TimeStampVector v = (TimeStampVector) vector;
          DateTime ts = new DateTime(((Timestamp) val).getTime()).withZoneRetainFields(DateTimeZone.UTC);
          long value = ts.getMillis();
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case "date": {
          DateVector v = (DateVector) vector;
          DateTime date = new DateTime(((Date)val).getTime()).withZoneRetainFields(DateTimeZone.UTC);
          long value = date.getMillis();
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case "decimal": {
          VarCharVector v = (VarCharVector) vector;
          byte[] value = ((HiveDecimal) val).toString().getBytes();
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        default:
          throw new UnsupportedOperationException("Could not determine type: " + selectedPartitionTypes.get(i));
      }
      vector.getMutator().setValueCount(recordCount);
    }
  }

  private Object convertPartitionType(String type, String value) {
    switch (type) {
      case "boolean":
        return Boolean.parseBoolean(value);
      case "tinyint":
        return Byte.parseByte(value);
      case "double":
        return Double.parseDouble(value);
      case "float":
        return Float.parseFloat(value);
      case "int":
        return Integer.parseInt(value);
      case "bigint":
        return Long.parseLong(value);
      case "smallint":
        return Short.parseShort(value);
      case "string":
        return value.getBytes();
      default:
        throw new UnsupportedOperationException("Could not determine type: " + type);
    }
  }

  public static TypeProtos.MajorType getMajorType(PrimitiveCategory pCat) {
    switch(pCat) {
      case BINARY:
        return Types.optional(TypeProtos.MinorType.VARBINARY);
      case BOOLEAN:
        return Types.optional(TypeProtos.MinorType.BIT);
      case BYTE:
        return Types.optional(TypeProtos.MinorType.TINYINT);
      case DECIMAL:
        return Types.optional(TypeProtos.MinorType.VARCHAR);
      case DOUBLE:
        return Types.optional(TypeProtos.MinorType.FLOAT8);
      case FLOAT:
        return Types.optional(TypeProtos.MinorType.FLOAT4);
      case INT:
        return Types.optional(TypeProtos.MinorType.INT);
      case LONG:
        return Types.optional(TypeProtos.MinorType.BIGINT);
      case SHORT:
        return Types.optional(TypeProtos.MinorType.SMALLINT);
      case STRING:
        return Types.optional(TypeProtos.MinorType.VARCHAR);
      case TIMESTAMP:
        return Types.optional(TypeProtos.MinorType.TIMESTAMP);
      case DATE:
        return Types.optional(TypeProtos.MinorType.DATE);
      default:
        throw new UnsupportedOperationException("Could not determine type: " + pCat);
    }
  }

  public boolean setValue(PrimitiveCategory pCat, ValueVector vv, int index, Object fieldValue) {
    switch(pCat) {
      case BINARY:
        return ((NullableVarBinaryVector) vv).getMutator().setSafe(index, (byte[]) fieldValue, 0, ((byte[]) fieldValue).length);
      case BOOLEAN:
        boolean isSet = (boolean) fieldValue;
        return ((NullableBitVector) vv).getMutator().setSafe(index, isSet ? 1 : 0 );
      case BYTE:
    	  return ((NullableTinyIntVector) vv).getMutator().setSafe(index, (byte) fieldValue);
      case DECIMAL:
        String value = ((HiveDecimal) fieldValue).toString();
        int strLen   = value.length();
        byte[] strBytes = value.getBytes();
        return ((NullableVarCharVector) vv).getMutator().setSafe(index, strBytes, 0, strLen);
      case DOUBLE:
    	  return ((NullableFloat8Vector) vv).getMutator().setSafe(index, (double) fieldValue);
      case FLOAT:
    	  return ((NullableFloat4Vector) vv).getMutator().setSafe(index, (float) fieldValue);
      case INT:
    	  return ((NullableIntVector) vv).getMutator().setSafe(index, (int) fieldValue);
      case LONG:
    	  return ((NullableBigIntVector) vv).getMutator().setSafe(index, (long) fieldValue);
      case SHORT:
    	  return ((NullableSmallIntVector) vv).getMutator().setSafe(index, (short) fieldValue);
      case STRING:
        int len = ((Text) fieldValue).getLength();
        byte[] bytes = ((Text) fieldValue).getBytes();
        return ((NullableVarCharVector) vv).getMutator().setSafe(index, bytes, 0, len);
      case TIMESTAMP:
        DateTime ts = new DateTime(((Timestamp) fieldValue).getTime()).withZoneRetainFields(DateTimeZone.UTC);
        return ((NullableTimeStampVector)vv).getMutator().setSafe(index, ts.getMillis());
      case DATE:
        DateTime date = new DateTime(((Date) fieldValue).getTime()).withZoneRetainFields(DateTimeZone.UTC);
        return ((NullableDateVector)vv).getMutator().setSafe(index, date.getMillis());
      default:
        throw new UnsupportedOperationException("Could not determine type");
    }
  }

  @Override
  public int next() {
    if (empty) {
      return 0;
    }

    for (ValueVector vv : vectors) {
      VectorAllocator.getAllocator(vv, 50).alloc(TARGET_RECORD_COUNT);
    }
    try {
      int recordCount = 0;
      if (redoRecord != null) {
        Object deSerializedValue = serde.deserialize((Writable) redoRecord);
        for (int i = 0; i < columnNames.size(); i++) {
          Object obj;
          String columnName = columnNames.get(i);
          if (primitiveCategories.get(i) == PrimitiveCategory.STRING) {
            obj = fieldInspectors.get(i).getPrimitiveWritableObject(sInspector.getStructFieldData(deSerializedValue, sInspector.getStructFieldRef(columnName)));
          } else {
            obj = fieldInspectors.get(i).getPrimitiveJavaObject(sInspector.getStructFieldData(deSerializedValue, sInspector.getStructFieldRef(columnName)));
          }
          boolean success = true;
          if( obj != null ) {
            success = setValue(primitiveCategories.get(i), vectors.get(i), recordCount, obj);
          }
          if (!success) {
            throw new DrillRuntimeException(String.format("Failed to write value for column %s", columnName));
          }
        }
        redoRecord = null;
        recordCount++;
      }
      while (recordCount < TARGET_RECORD_COUNT && reader.next(key, value)) {
        Object deSerializedValue = serde.deserialize((Writable) value);
        for (int i = 0; i < columnNames.size(); i++) {
          Object obj;
          String columnName = columnNames.get(i);
          if (primitiveCategories.get(i) == PrimitiveCategory.STRING) {
            obj = fieldInspectors.get(i).getPrimitiveWritableObject(sInspector.getStructFieldData(deSerializedValue, sInspector.getStructFieldRef(columnName)));
          } else {
            obj = fieldInspectors.get(i).getPrimitiveJavaObject(sInspector.getStructFieldData(deSerializedValue, sInspector.getStructFieldRef(columnName)));
          }
          boolean success = true;
          if( obj != null ) {
            success = setValue(primitiveCategories.get(i), vectors.get(i), recordCount, obj);
          }
          if (!success) {
            redoRecord = value;
            if (partition != null) populatePartitionVectors(recordCount);
            for (ValueVector v : vectors) {
              v.getMutator().setValueCount(recordCount);
            }
            if (partition != null) populatePartitionVectors(recordCount);
            return recordCount;
          }
        }
        recordCount++;
      }
      for (ValueVector v : vectors) {
        v.getMutator().setValueCount(recordCount);
      }
      if (partition != null) populatePartitionVectors(recordCount);
      return recordCount;
    } catch (IOException | SerDeException e) {
      throw new DrillRuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
  }
}
