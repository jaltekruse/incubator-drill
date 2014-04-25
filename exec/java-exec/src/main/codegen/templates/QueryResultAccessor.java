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

<@pp.dropOutputFile />
<@pp.changeOutputFile name="org/apache/drill/exec/record/QueryResultAccessor.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.record;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.exception.*;

import java.util.*;

/**
 *
 * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
 */
public class QueryResultAccessor {
  private RecordBatchLoader currentRecordBatchLoader;
  private QueryResultBatch currentBatch;
  private int currentBatchIndex;
  private ValueVector[] vectors;

  private List<QueryResultBatch> results;
  private BufferAllocator allocator;

  public QueryResultAccessor(List<QueryResultBatch> results, BufferAllocator allocator) {
    this.results = results;

    this.currentBatchIndex = -1;
  }

  /**
   * Go to the next RecordBatch.
   * @return Returns true if a batch is available.
   * @throws SchemaChangeException
   */
  public boolean nextBatch() throws SchemaChangeException {
    // release existing batch loader
    if (currentRecordBatchLoader != null)
      currentRecordBatchLoader.clear();

    if (currentBatchIndex < results.size() - 1) {
      currentBatchIndex++;

      // get the next QueryResultBatch
      currentBatch = results.get(currentBatchIndex);

      // load the current QueryResultBatch
      currentRecordBatchLoader = new RecordBatchLoader(allocator);
      currentRecordBatchLoader.load(currentBatch.getHeader().getDef(), currentBatch.getData());

      initVectorWrappers();
      return true;
    }

    return false;
  }

  /**
   * Return the number of records in current record batch
   * @return
   */
  public int getRowCount() {
    // TODO: Error checking.
    return currentBatch.getHeader().getRowCount();
  }

  private void initVectorWrappers() {
    BatchSchema schema = currentRecordBatchLoader.getSchema();
    vectors = new ValueVector[schema.getFieldCount()];
    int fieldId = 0;
    Class<?> vectorClass;
    for (MaterializedField field : schema) {
      vectorClass = TypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getType().getMode());
      vectors[fieldId] = currentRecordBatchLoader.getValueAccessorById(fieldId, vectorClass).getValueVector();
      fieldId++;
    }
  }

<#list vv.types as type>
  <#list type.minor as minor>
    <#list vv.modes as mode>

  public void getFieldById(int fieldId, int rowIndex, ${mode.prefix}${minor.class}Holder holder){
    ((${mode.prefix}${minor.class}Vector) vectors[fieldId]).getAccessor().get(rowIndex, holder);
  }

    </#list>
  </#list>
</#list>
}
