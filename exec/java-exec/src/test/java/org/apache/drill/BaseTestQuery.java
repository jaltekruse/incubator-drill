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
package org.apache.drill;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.PrintingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter;
import org.apache.drill.exec.client.QuerySubmitter.Format;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.record.QueryResultAccessor;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.util.VectorUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class BaseTestQuery extends ExecTest{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);

  public final TestRule resetWatcher = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      try {
        resetClientAndBit();
      } catch (Exception e1) {
        throw new RuntimeException("Failure while resetting client.", e1);
      }
    }
  };

  static DrillClient client;
  static Drillbit bit;
  static RemoteServiceSet serviceSet;
  static DrillConfig config;
  static QuerySubmitter submitter = new QuerySubmitter();

  static void resetClientAndBit() throws Exception{
    closeClient();
    openClient();
  }

  @BeforeClass
  public static void openClient() throws Exception{
    config = DrillConfig.create();
    serviceSet = RemoteServiceSet.getLocalServiceSet();
    bit = new Drillbit(config, serviceSet);
    bit.run();
    client = new DrillClient(config, serviceSet.getCoordinator());
    client.connect();
  }

  protected BufferAllocator getAllocator(){
    return client.getAllocator();
  }

  @AfterClass
  public static void closeClient() throws IOException{
    if(client != null) client.close();
    if(bit != null) bit.close();
    if(serviceSet != null) serviceSet.close();
  }


  protected void testSqlWithValidator(String sql, Validator validator) throws Exception{
    testRunAndReturn(QueryType.SQL, sql, validator);
  }

  protected void testLogicalWithValidator(String logical, Validator validator) throws Exception{
    testRunAndReturn(QueryType.LOGICAL, logical, validator);
  }

  protected void testPhysicalWithValidator(String physical, Validator validator) throws Exception{
    testRunAndReturn(QueryType.PHYSICAL, physical, validator);
  }

  private void testRunAndReturn(QueryType type, String query, Validator validator) throws Exception{
    query = query.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    List<QueryResultBatch> results = client.runQuery(type, query);
    QueryResultAccessor provider = new QueryResultAccessor(results, getAllocator());
    validator.validate(provider);
    for(QueryResultBatch b : results) {
      b.release();
    }
  }

  private int testRunAndPrint(QueryType type, String query) throws Exception{
    query = query.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    PrintingResultsListener resultListener = new PrintingResultsListener(Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH);
    client.runQuery(type, query, resultListener);
    return resultListener.await();
  }

  protected void testWithListener(QueryType type, String query, UserResultsListener resultListener){
    query = query.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    client.runQuery(type, query, resultListener);
  }

  protected void test(String query) throws Exception{
    String[] queries = query.split(";");
    for(String q : queries){
      if(q.trim().isEmpty()) continue;
      testRunAndPrint(QueryType.SQL, q);
    }
  }

  protected int testLogical(String query) throws Exception{
    return testRunAndPrint(QueryType.LOGICAL, query);
  }

  protected int testPhysical(String query) throws Exception{
    return testRunAndPrint(QueryType.PHYSICAL, query);
  }

  protected void testPhysicalFromFile(String file) throws Exception{
    testPhysical(getFile(file));
  }
  protected void testLogicalFromFile(String file) throws Exception{
    testLogical(getFile(file));
  }
  protected void testSqlFromFile(String file) throws Exception{
    test(getFile(file));
  }


  protected String getFile(String resource) throws IOException{
    URL url = Resources.getResource(resource);
    if(url == null){
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  interface Validator {
    void validate(QueryResultAccessor accessor) throws Exception;
  }
}