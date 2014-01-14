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
package org.apache.drill.exec.physical.impl;

import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;

public class OptionValueReaderBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OptionValueReaderBatch.class);

  final Map<MaterializedField, ValueVector> fieldVectorMap = Maps.newHashMap();

  private final VectorContainer container = new VectorContainer();
  private int recordCount;
  private boolean schemaChanged = true;
  private final FragmentContext context;
  private BatchSchema schema;
  private String optionName;
  private final Mutator mutator = new Mutator();
  // need to send value on first request for data, and then the NONE message to indicate no
  // more data, used to track option value has been sent
  private boolean valueSent = false;

  public OptionValueReaderBatch(FragmentContext context, String optionName) throws ExecutionSetupException {
    this.context = context;
    this.optionName = optionName;
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return schema;
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public void kill() {
    releaseAssets();
  }

  private void releaseAssets() {
    container.clear();
  }

  @Override
  public IterOutcome next() {
    if (! valueSent) {
      MaterializedField field = MaterializedField.create(new SchemaPath("option_value", ExpressionPosition.UNKNOWN),
          Types.required(TypeProtos.MinorType.VARBINARY));
      VarBinaryVector vector = (VarBinaryVector) TypeHelper.getNewVector(field, context.getAllocator());
      vector.allocateNew(500, 1);
      mutator.addField(vector);
      Object optionVal = context.getConnection().getSessionLevelOption(optionName);
      if (optionVal == null ){
        optionVal = context.getDrillbitContext().getGlobalDrillOptions().getOption(optionName);
      }
      byte[] strOptVal = optionVal.toString().getBytes(Charset.forName("UTF-8"));
      vector.getMutator().set(0, strOptVal);
      vector.getMutator().setValueCount(1);
      this.recordCount = 1;
      container.setRecordCount(1);
      try {
        mutator.setNewSchema();
      } catch (SchemaChangeException e) {
        this.context.fail(e);
        releaseAssets();
        return IterOutcome.STOP;
      }
      valueSent = true;
      return IterOutcome.OK_NEW_SCHEMA;
    }
    else {
      return IterOutcome.NONE;
    }
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return container.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    return container.getValueAccessorById(fieldId, clazz);
  }

  private class Mutator implements OutputMutator {

    public void removeField(MaterializedField field) throws SchemaChangeException {
      ValueVector vector = fieldVectorMap.remove(field);
      if (vector == null) throw new SchemaChangeException("Failure attempting to remove an unknown field.");
      container.remove(vector);
      vector.close();
    }

    public void addField(ValueVector vector) {
      container.add(vector);
      fieldVectorMap.put(vector.getField(), vector);
    }

    @Override
    public void removeAllFields() {
      for(VectorWrapper<?> vw : container){
        vw.clear();
      }
      container.clear();
      fieldVectorMap.clear();
    }

    @Override
    public void setNewSchema() throws SchemaChangeException {
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      schema = container.getSchema();
      OptionValueReaderBatch.this.schemaChanged = true;
    }

  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  @Override
  public void cleanup() {
    container.clear();
  }

}

