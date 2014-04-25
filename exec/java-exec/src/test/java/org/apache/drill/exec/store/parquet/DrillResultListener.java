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

import static junit.framework.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.ResultProvider;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.SettableFuture;

public class DrillResultListener implements UserResultsListener {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillResultListener.class);

  private SettableFuture<Void> future = SettableFuture.create();
  int count = 0;
  int totalRecords;
  boolean testValues;
  BufferAllocator allocator;
  int batchCounter = 1;

  // stores a map between column names and total record counts
  // may want to expand this to a map between column names and an object
  // type if there is other metadata to track about a column
  HashMap<String, Integer> valuesChecked = new HashMap<>();
  ResultProvider provider;

  DrillResultListener(BufferAllocator allocator, ResultProvider provider, int totalRecords, boolean testValues){
    this.allocator = allocator;
    this.provider = provider;
    this.totalRecords = totalRecords;
    this.testValues = testValues;
  }

  @Override
  public void submissionFailed(RpcException ex) {
    logger.debug("Submission failed.", ex);
    future.setException(ex);
  }

  private <T> void assertField(ValueVector valueVector, int index, Object value, String name) {
    assertField(valueVector, index, value, name, 0);
  }

  @SuppressWarnings("unchecked")
  private <T> void assertField(ValueVector valueVector, int index, T value, String name, int parentFieldId) {

    T val = (T) valueVector.getAccessor().getObject(index);
    if (val instanceof byte[]) {
      assertEquals(true, Arrays.equals((byte[]) value, (byte[]) val));
    } else {
      assertEquals(value, val);
    }
  }

  @Override
  synchronized public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
    logger.debug("result arrived in test batch listener.");
    if(result.getHeader().getIsLastChunk()){
      future.set(null);
    }
    int columnValCounter = 0;
    count += result.getHeader().getRowCount();
    boolean schemaChanged = false;
    RecordBatchLoader batchLoader = new RecordBatchLoader(allocator);
    try {
      schemaChanged = batchLoader.load(result.getHeader().getDef(), result.getData());
    } catch (SchemaChangeException e) {
      throw new RuntimeException(e);
    }

    int recordCount = 0;
    // print headers.
    // TODO - assuming unchanged schema, an interface method can be added to allow users of this test system
    // to define behavior on a schema change
    if (schemaChanged) {}

    int columnIndex = 0;
    for (VectorWrapper vw : batchLoader) {
      ValueVector vv = vw.getValueVector();
      //currentField = props.fields.get(vv.getField().getAsSchemaPath().getRootSegment().getPath());

      // check if the column has been read before
      if ( ! valuesChecked.containsKey(vv.getField().getAsSchemaPath().getRootSegment().getPath())){
        // if it has not been read place it in the map
        valuesChecked.put(vv.getField().getAsSchemaPath().getRootSegment().getPath(), 0);
        columnValCounter = 0;
      } else {
        // pull the column value count out of the map
        columnValCounter = valuesChecked.get(vv.getField().getAsSchemaPath().getRootSegment().getPath());
      }
      for (int j = 0; j < vv.getAccessor().getValueCount(); j++) {
        if (testValues){
          assertField(vv, j,
              provider.get(columnValCounter + j, columnIndex), vv.getField().toExpr() + "/");
        }
      }
      if (ParquetRecordReaderTest.VERBOSE_DEBUG){
        System.out.println("\n" + vv.getAccessor().getValueCount());
      }
      valuesChecked.remove(vv.getField().getAsSchemaPath().getRootSegment().getPath());
      columnValCounter += vv.getAccessor().getValueCount();
      valuesChecked.put(vv.getField().getAsSchemaPath().getRootSegment().getPath(), columnValCounter);
    }

    // check all columns have recieved the same number of values so far
    assert valuesChecked.keySet().size() > 0 : "No columns returned";
    int valuesSoFar = valuesChecked.get(valuesChecked.keySet().iterator().next());
    for (String s : valuesChecked.keySet()) {
      assert valuesChecked.get(s).equals(valuesSoFar) : "mismatch between value count of columns";
      valuesSoFar = valuesChecked.get(s);
    }

    if (ParquetRecordReaderTest.VERBOSE_DEBUG){
      printRowWise(batchLoader);
    }
    batchCounter++;
    if(result.getHeader().getIsLastChunk()){
      // ensure the right number of columns was returned, especially important to ensure selective column read is working
      //assert valuesChecked.keySet().size() ==  : "Unexpected number of output columns from parquet scan,";
      for (String s : valuesChecked.keySet()) {
        try {
          assertEquals("Record count incorrect for column: " + s, totalRecords, (long) valuesChecked.get(s));
        } catch (AssertionError e) { submissionFailed(new RpcException(e)); }
      }

      assert valuesChecked.keySet().size() > 0;
      result.release();
      future.set(null);
    }else{
      result.release();
    }
  }

  public void printColumnWise(RecordBatchLoader batchLoader) {
    FieldInfo currentField;
    for (VectorWrapper vw : batchLoader) {
      ValueVector vv = vw.getValueVector();
      //currentField = props.fields.get(vv.getField().getAsSchemaPath().getRootSegment().getPath());
      if (ParquetRecordReaderTest.VERBOSE_DEBUG){
        System.out.println("\n" + vv.getField().toExpr());
      }
      for (int j = 0; j < vv.getAccessor().getValueCount(); j++) {
        if (ParquetRecordReaderTest.VERBOSE_DEBUG){
          if (vv.getAccessor().getObject(j) instanceof byte[]){
            System.out.print("[len:" + ((byte[]) vv.getAccessor().getObject(j)).length + " - (");
            for (int k = 0; k <  ((byte[]) vv.getAccessor().getObject(j)).length; k++){
              System.out.print((char)((byte[])vv.getAccessor().getObject(j))[k] + ",");
            }
            System.out.print(") ]");
          }
          else{
            System.out.print(Strings.padStart(vv.getAccessor().getObject(j) + "", 20, ' ') + " ");
          }
          System.out.print(", " + (j % 25 == 0 ? "\n batch:" + batchCounter + " v:" + j + " - " : ""));
        }
      }
    }
  }

  public void printRowWise(RecordBatchLoader batchLoader) {
    for (int i = 0; i < batchLoader.getRecordCount(); i++) {
      if (i % 50 == 0){
        System.out.println();
        for (VectorWrapper vw : batchLoader) {
          ValueVector v = vw.getValueVector();
          System.out.print(Strings.padStart(v.getField().getAsSchemaPath().getRootSegment().getPath(), 20, ' ') + " ");

        }
        System.out.println();
        System.out.println();
      }

      for (VectorWrapper vw : batchLoader) {
        ValueVector v = vw.getValueVector();
        if (v.getAccessor().getObject(i) instanceof byte[]){
          System.out.print("[len:" + ((byte[]) v.getAccessor().getObject(i)).length + " - (");
          for (int j = 0; j <  ((byte[]) v.getAccessor().getObject(i)).length; j++){
            System.out.print(((byte[])v.getAccessor().getObject(i))[j] + ",");
          }
          System.out.print(") ]");
        }
        else{
          System.out.print(Strings.padStart(v.getAccessor().getObject(i) + "", 20, ' ') + " ");
        }
      }
      System.out.println();
    }
  }

  public void getResults() throws RpcException{
    try{
      future.get();
    }catch(Throwable t){
      throw RpcException.mapException(t);
    }
  }

  @Override
  public void queryIdArrived(UserBitShared.QueryId queryId) {
  }
}
