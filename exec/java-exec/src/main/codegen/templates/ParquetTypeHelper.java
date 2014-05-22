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

import parquet.format.ConvertedType;
import parquet.schema.DecimalMetadata;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="org/apache/drill/exec/store/parquet/ParquetTypeHelper.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.store.parquet;

import io.netty.buffer.SwappedByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.parquet2.*;
import org.apache.drill.exec.store.parquet2.DrillParquetRecordConverter.DrillPrimitiveConverter;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.expr.holders.*;
import parquet.schema.OriginalType;
import parquet.schema.DecimalMetadata;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;
import parquet.io.api.Binary;

import java.util.HashMap;
import java.util.Map;

public class ParquetTypeHelper {
  private static Map<MinorType,PrimitiveTypeName> typeMap;
  private static Map<DataMode,Repetition> modeMap;
  private static Map<MinorType,OriginalType> originalTypeMap;

  static {
    typeMap = new HashMap();

    <#list vv.types as type>
    <#list type.minor as minor>
    <#if    minor.class == "TinyInt" ||
            minor.class == "UInt1" ||
            minor.class == "UInt2" ||
            minor.class == "SmallInt" ||
            minor.class == "Int" ||
            minor.class == "Time" ||
            minor.class == "IntervalYear" ||
            minor.class == "Decimal9" ||
            minor.class == "UInt4">
                    typeMap.put(MinorType.${minor.class?upper_case}, PrimitiveTypeName.INT32);
    <#elseif
            minor.class == "Float4">
                    typeMap.put(MinorType.${minor.class?upper_case}, PrimitiveTypeName.FLOAT);
    <#elseif
            minor.class == "BigInt" ||
            minor.class == "Decimal18" ||
            minor.class == "TimeStamp" ||
            minor.class == "Date" ||
            minor.class == "UInt8">
                    typeMap.put(MinorType.${minor.class?upper_case}, PrimitiveTypeName.INT64);
    <#elseif
            minor.class == "Float8">
                    typeMap.put(MinorType.${minor.class?upper_case}, PrimitiveTypeName.DOUBLE);
    <#elseif
            minor.class == "Bit">
                    typeMap.put(MinorType.${minor.class?upper_case}, PrimitiveTypeName.BOOLEAN);
    <#elseif
            minor.class == "TimeTZ" ||
            minor.class == "TimeStampTZ" ||
            minor.class == "IntervalDay" ||
            minor.class == "Interval" ||
            minor.class == "Decimal28Dense" ||
            minor.class == "Decimal38Dense" ||
            minor.class == "Decimal28Sparse" ||
            minor.class == "Decimal38Sparse">
                    typeMap.put(MinorType.${minor.class?upper_case}, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
    <#elseif
            minor.class == "VarChar" ||
            minor.class == "Var16Char" ||
            minor.class == "VarBinary" >
                    typeMap.put(MinorType.${minor.class?upper_case}, PrimitiveTypeName.BINARY);
    </#if>
    </#list>
    </#list>

    modeMap = new HashMap();

    modeMap.put(DataMode.REQUIRED, Repetition.REQUIRED);
    modeMap.put(DataMode.OPTIONAL, Repetition.OPTIONAL);
    modeMap.put(DataMode.REPEATED, Repetition.REPEATED);

    originalTypeMap = new HashMap();

    <#list vv.types as type>
    <#list type.minor as minor>
            <#if minor.class.startsWith("Decimal")>
            originalTypeMap.put(MinorType.${minor.class?upper_case},OriginalType.DECIMAL);
            </#if>
    </#list>
    </#list>
            originalTypeMap.put(MinorType.VARCHAR, OriginalType.UTF8);
            originalTypeMap.put(MinorType.DATE, OriginalType.DATE);
            originalTypeMap.put(MinorType.TIME, OriginalType.TIME);
            originalTypeMap.put(MinorType.TIMESTAMP, OriginalType.TIMESTAMP);
            originalTypeMap.put(MinorType.TIMESTAMPTZ, OriginalType.TIMESTAMPTZ);
  }

  public static PrimitiveTypeName getPrimitiveTypeNameForMinorType(MinorType minorType) {
    return typeMap.get(minorType);
  }

  public static Repetition getRepetitionForDataMode(DataMode dataMode) {
    return modeMap.get(dataMode);
  }

  public static OriginalType getOriginalTypeForMinorType(MinorType minorType) {
    return originalTypeMap.get(minorType);
  }

  public static DecimalMetadata getDecimalMetadataForField(MaterializedField field) {
    switch(field.getType().getMinorType()) {
      case DECIMAL9:
      case DECIMAL18:
      case DECIMAL28SPARSE:
      case DECIMAL28DENSE:
      case DECIMAL38SPARSE:
      case DECIMAL38DENSE:
        return new DecimalMetadata(field.getPrecision(), field.getScale());
      default:
        return null;
    }
  }

  public static int getLengthForMinorType(MinorType minorType) {
    switch(minorType) {
      case DECIMAL28SPARSE:
        return 12;
      case DECIMAL38SPARSE:
        return 16;
      default:
        return 0;
    }
  }

  public static DrillPrimitiveConverter getConverterForVector(ValueVector vector) {
    switch(vector.getField().getType().getMinorType()){
    <#list vv.types as type>
    <#list type.minor as minor>
    case ${minor.class?upper_case}:
    switch (vector.getField().getType().getMode()) {
      case REQUIRED:
        return new ${minor.class}Converter((${minor.class}Vector) vector);
      case OPTIONAL:
        return new Nullable${minor.class}Converter((Nullable${minor.class}Vector) vector);
//      case REPEATED:
//        return new Repeated${minor.class}Converter((Nullable${minor.class}Vector) vector);
      default:
      throw new UnsupportedOperationException();
    }
    </#list>
    </#list>
    default:
    throw new UnsupportedOperationException();
    }
  }

  <#list vv.types as type>
  <#list type.minor as minor>
  public static class ${minor.class}Converter extends DrillPrimitiveConverter {
        ${minor.class}Holder holder = new ${minor.class}Holder();
        ${minor.class}Vector vector;

        ${minor.class}Converter(ValueVector vector) {
        this.vector = (${minor.class}Vector) vector;
        }

        @Override
        public void reset() {
        }

        @Override
        public boolean write(int index) {
          return vector.getMutator().setSafe(index, holder);
        }

        <#if minor.class == "Int">
        @Override
        public void addInt(int value) {
        holder.value = value;
        }
        <#elseif minor.class == "BigInt">
        @Override
        public void addLong(long value) {
        holder.value = value;
        }
        <#elseif minor.class == "Float4">
        @Override
        public void addFloat(float value) {
        holder.value = value;
        }
        <#elseif minor.class == "Float8">
        @Override
        public void addDouble(double value) {
        holder.value = value;
        }
        <#elseif minor.class == "VarChar">
        @Override
        public void addBinary(Binary value) {
        SwappedByteBuf buf = new SwappedByteBuf(Unpooled.wrappedBuffer(value.getBytes()));
        holder.buffer = buf;
        holder.start = 0;
        holder.end = value.length();
        }
        <#elseif minor.class == "VarBinary">
        @Override
        public void addBinary(Binary value) {
        SwappedByteBuf buf = new SwappedByteBuf(Unpooled.wrappedBuffer(value.getBytes()));
        holder.buffer = buf;
        holder.start = 0;
        holder.end = value.length();
        }
        </#if>

  }
  </#list>
  </#list>

 <#list vv.types as type>
  <#list type.minor as minor>
  public static class Nullable${minor.class}Converter extends DrillPrimitiveConverter {
        Nullable${minor.class}Holder holder = new Nullable${minor.class}Holder();
        Nullable${minor.class}Vector vector;

        Nullable${minor.class}Converter(ValueVector vector) {
        this.vector = (Nullable${minor.class}Vector) vector;
        this.holder.isSet = 1;
        }

        @Override
        public void reset() {
          holder.isSet = 0;
        }

        @Override
        public boolean write(int index) {
          if (holder.isSet == 0) return vector.getValueCapacity() > index;
          return vector.getMutator().setSafe(index, holder);
        }

        <#if minor.class == "Int">
        @Override
        public void addInt(int value) {
      holder.isSet = 1;
        holder.value = value;
        }
        <#elseif minor.class == "BigInt">
        @Override
        public void addLong(long value) {
      holder.isSet = 1;
        holder.value = value;
        }
        <#elseif minor.class == "Float4">
        @Override
        public void addFloat(float value) {
      holder.isSet = 1;
        holder.value = value;
        }
        <#elseif minor.class == "Float8">
        @Override
        public void addDouble(double value) {
      holder.isSet = 1;
        holder.value = value;
        }
        <#elseif minor.class == "VarChar">
        @Override
        public void addBinary(Binary value) {
      holder.isSet = 1;
        SwappedByteBuf buf = new SwappedByteBuf(Unpooled.wrappedBuffer(value.getBytes()));
        holder.buffer = buf;
        holder.start = 0;
        holder.end = value.length();
        }
        <#elseif minor.class == "VarBinary">
        @Override
        public void addBinary(Binary value) {
      holder.isSet = 1;
        SwappedByteBuf buf = new SwappedByteBuf(Unpooled.wrappedBuffer(value.getBytes()));
        holder.buffer = buf;
        holder.start = 0;
        holder.end = value.length();
        }
        </#if>

  }
  </#list>
  </#list>

}
