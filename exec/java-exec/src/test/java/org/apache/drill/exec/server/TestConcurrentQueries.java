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

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.client.PrintingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter;
import org.apache.drill.exec.fn.interp.TestConstantFolding;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.util.VectorUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.drill.QueryTestUtil.normalizeQuery;

public class TestConcurrentQueries extends BaseTestQuery {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testLaggingFragment() throws Exception {
    new TestConstantFolding.SmallFileCreator(folder).createFiles(1, 100_000, "csv");
    test("alter session set `planner.slice_target` = 1");
//    String slowQuery = "select test_debugging_function_wait(columns[0]) from dfs.`/Users/jaltekruse/test_data_drill/bunch_o_csv`";
//    String slowQuery = "select test_debugging_function_wait(columns[0]) from dfs.`" + folder.getRoot().toPath() +"/bigfile` order by columns[2]";
    String slowQuery = "select columns[0] from dfs.`" + folder.getRoot().toPath() +"/bigfile` order by columns[2]";
    final String query = normalizeQuery(slowQuery);
    PrintingResultsListener resultListener =
        new PrintingResultsListener(client.getConfig(), QuerySubmitter.Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH);
    PrintingResultsListener resultListener2 =
        new PrintingResultsListener(client.getConfig(), QuerySubmitter.Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH);
    client.runQuery(UserBitShared.QueryType.SQL, query, resultListener);
    try {
      Thread.sleep(20000);
      client.runQuery(UserBitShared.QueryType.SQL, query, resultListener2);
      resultListener.await();
      resultListener2.await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
