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
package org.apache.drill.exec.store.parquet2;

import com.google.common.collect.Lists;
import io.netty.buffer.SwappedByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.store.parquet.ParquetTypeHelper;
import org.apache.drill.exec.vector.*;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.MessageType;

import java.util.List;

public class DrillParquetRecordConverter  extends GroupConverter {

  MessageType schema;
  List<ValueVector> vectors;
  List<Converter> converters;
  DrillParquetRecord record = new DrillParquetRecord();

  public DrillParquetRecordConverter(List<ValueVector> vectors, MessageType schema) {
    this.schema = schema;
    this.vectors = vectors;
    converters = Lists.newArrayList();
    for (ValueVector v : vectors) {
      DrillPrimitiveConverter converter = ParquetTypeHelper.getConverterForVector(v);
      record.addConverter(converter);
      converters.add(converter);
    }
  }

  public DrillParquetRecord getCurrentRecord() {
    return record;
  }

  @Override
  public Converter getConverter(int i) {
    return converters.get(i);
  }

  @Override
  public void start() {
    record.success = true;
  }

  @Override
  public void end() {
    record.write();
  }

  public static abstract class DrillPrimitiveConverter extends PrimitiveConverter {
    public abstract boolean write(int index);
  }
}
