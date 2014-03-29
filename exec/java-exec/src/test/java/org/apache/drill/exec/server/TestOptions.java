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
package org.apache.drill.exec.server;

import com.google.common.io.Files;
import org.apache.commons.codec.Charsets;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestOptions extends PopUnitTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOptions.class);

  @Test
  public void testSessionOptionSet() throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); DrillClient client =
        new DrillClient(CONFIG, serviceSet.getCoordinator());DrillClient client2 =
        new DrillClient(CONFIG, serviceSet.getCoordinator());){
      bit1.run();
      runOptionTests(client, client2);
    }
  }

  public static void runOptionTests(DrillClient client, DrillClient client2) throws Exception {
    client.connect();
    String setGlobalAndSession =
        "global : { \"EXPLAIN_PLAN_LEVEL\" : \"logical\" }, " +
            "session: { \"explain_plan_format\" : \"xml\" }";
    String plan = Files.toString(FileUtils.getResourceAsFile("/server/options_set.json"), Charsets.UTF_8);
    String setGlobalPlan = plan.replace("&REPLACED_IN_TEST&", setGlobalAndSession);
    TestOptions.runQuery(setGlobalPlan, "`default`", "EXPLAIN_PLAN_LEVEL", "logical", client);

    plan = Files.toString(FileUtils.getResourceAsFile("/server/options_session_check.json"), Charsets.UTF_8);
    TestOptions.runQuery(plan, "`default`", "explain_plan_level", "logical", client);

    client2.connect();
    TestOptions.runQuery(plan, "`session`", "EXPLAIN_PLAN_FORMAT", "xml", client);
    TestOptions.runQuery(plan, "`default`", "explain_plan_level", "logical", client2);
    TestOptions.runQuery(plan, "`session`", "explain_plan_format", null, client2);

  }

  public static void runQuery(String plan, String scope, String optionName, String expectedVal,
                       DrillClient client) throws Exception {
    List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
        plan);
    int count = 0;
    boolean valueChecked= false;
    RecordBatchLoader batchLoader = new RecordBatchLoader(new TopLevelAllocator());
    for(QueryResultBatch b : results){
      boolean schemaChanged = batchLoader.load(b.getHeader().getDef(), b.getData());
      count += b.getHeader().getRowCount();
      String output = "";
      int explainPlanIndex = -1;
      ValueVector sessionVector = null;
      for (VectorWrapper vw : batchLoader) {
        ValueVector vv = vw.getValueVector();
        output += vv.getField().toExpr() + ":";
        if (vv.getField().toExpr().equals(scope)){
          sessionVector = vv;
        }
        else if (vv.getField().toExpr().equals("`name`")){
          for (int i = 0; i < vv.getAccessor().getValueCount(); i++ ) {
            if (((String)vv.getAccessor().getObject(i)).equalsIgnoreCase(optionName)){
              explainPlanIndex = i;
              valueChecked = true;
            }
          }
        }
      }
      logger.debug(output);
      if (explainPlanIndex != -1){
        String val = (String) sessionVector.getAccessor().getObject(explainPlanIndex);
        if ( ! (val == null && expectedVal == null)
              && ! expectedVal.equalsIgnoreCase(val)){
          throw new Exception("expecting value [" + expectedVal + "] but received value [" + val + "]");
        }
      }
    }
    assertTrue(valueChecked);
  }
}
