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
package org.apache.drill.exec.store.options;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.DistributedGlobalOptions;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.VarCharVector;

import java.nio.charset.Charset;
import java.util.Iterator;

public class OptionValueRecordReader implements RecordReader {

  // in bytes
  private static final int DEFAULT_VECTOR_LEN = 5000;
  private static final int DEFAULT_VECTOR_VAL_LIMIT = 30;

  FragmentContext context;
  VarCharVector nameVector;
  VarCharVector categoryVector;
  NullableVarCharVector sessionValVector;
  VarCharVector defaultValVector;
  Iterator<String> optionNameIterator;
  DistributedGlobalOptions distributedGlobalOptions;

  public OptionValueRecordReader(FragmentContext context) {
    this.context = context;
    this.optionNameIterator = context.getDrillbitContext().getGlobalDrillOptions().getAvailableOptionNamesIterator();
    distributedGlobalOptions = context.getDrillbitContext().getGlobalDrillOptions();
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    MaterializedField field = MaterializedField.create(new SchemaPath("name", ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.VARCHAR));
    nameVector = (VarCharVector) TypeHelper.getNewVector(field, context.getAllocator());
    nameVector.allocateNew(500, 50);
    field = MaterializedField.create(new SchemaPath("category", ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.VARCHAR));
    categoryVector = (VarCharVector) TypeHelper.getNewVector(field, context.getAllocator());
    categoryVector.allocateNew(500, 50);
    field = MaterializedField.create(new SchemaPath("session", ExpressionPosition.UNKNOWN),
        Types.optional(TypeProtos.MinorType.VARCHAR));
    sessionValVector = (NullableVarCharVector) TypeHelper.getNewVector(field, context.getAllocator());
    sessionValVector.allocateNew(500, 50);
    field = MaterializedField.create(new SchemaPath("default", ExpressionPosition.UNKNOWN),
        Types.required(TypeProtos.MinorType.VARCHAR));
    defaultValVector = (VarCharVector) TypeHelper.getNewVector(field, context.getAllocator());
    defaultValVector.allocateNew(500, 50);
    try {
      output.addField(nameVector);
      output.addField(categoryVector);
      output.addField(sessionValVector);
      output.addField(defaultValVector);
      output.setNewSchema();
    } catch (SchemaChangeException e) {
      throw new RuntimeException(e);
    }
  }

  private void reset(){
    sessionValVector.allocateNew(DEFAULT_VECTOR_LEN, DEFAULT_VECTOR_VAL_LIMIT);
    defaultValVector.allocateNew(DEFAULT_VECTOR_LEN, DEFAULT_VECTOR_VAL_LIMIT);
    categoryVector.allocateNew(DEFAULT_VECTOR_LEN, DEFAULT_VECTOR_VAL_LIMIT);
    nameVector.allocateNew(DEFAULT_VECTOR_LEN, DEFAULT_VECTOR_VAL_LIMIT);
  }

  @Override
  public int next() {
    reset();
    int i = 0;
    Charset charSet = java.nio.charset.Charset.forName("UTF-8");
    Object sessionVal;
    String optName;
    while ( i < DEFAULT_VECTOR_VAL_LIMIT && optionNameIterator.hasNext()){
      optName = optionNameIterator.next();
      nameVector.getMutator().set(i, optName.getBytes(charSet));
      defaultValVector.getMutator().set(i, distributedGlobalOptions.getOptionStringVal(optName).getBytes(charSet));
      categoryVector.getMutator().set(i,
          context.getDrillbitContext().getGlobalDrillOptions()
              .getOptionCategory(optName).getName().getBytes(charSet));
      sessionVal = context.getConnection()
          .getSessionLevelOption(optName);
      if (sessionVal != null){
        sessionValVector.getMutator().set(i,
            context.getConnection().getSessionLevelOptionStringVal(optName).getBytes(charSet));
      }
      i++;
    }
    nameVector.getMutator().setValueCount(i);
    defaultValVector.getMutator().setValueCount(i);
    sessionValVector.getMutator().setValueCount(i);
    categoryVector.getMutator().setValueCount(i);
    return i;
  }

  @Override
  public void cleanup() {
    nameVector.clear();
    defaultValVector.clear();
    categoryVector.clear();
    sessionValVector.clear();
  }
}
