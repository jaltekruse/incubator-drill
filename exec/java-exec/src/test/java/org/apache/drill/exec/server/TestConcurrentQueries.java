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

import com.google.common.collect.Lists;
import mockit.Injectable;
import org.apache.commons.lang.time.StopWatch;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.client.PrintingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter;
import org.apache.drill.exec.fn.interp.TestConstantFolding;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.work.fragment.NonRootFragmentManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.drill.QueryTestUtil.normalizeQuery;

public class TestConcurrentQueries extends BaseTestQuery {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(500000);

  private static class StartFragmentThread implements Runnable {
    BitControl.PlanFragment fragment;
    StartFragmentThread(BitControl.PlanFragment fragment) {
      this.fragment = fragment;
    }

    @Override
    public void run() {
      StopWatch watch = new StopWatch();
      watch.start();
      final NonRootFragmentManager manager;
      try {
        manager = new NonRootFragmentManager(fragment, getDrillbitContext());
      } catch (ExecutionSetupException e) {
        throw new RuntimeException(e);
      }
      System.out.println("intermediate fragment startup time: " + watch.getTime());
      manager.cancel();
    }
  }

  @Test
  // cast to bigint.
  public void testSetupLaggingFragment() throws Throwable{
    File file = new File("/Users/jaltekruse/Dropbox/drill/3480-long-frag-startup-binary/plan_fragment846296378487392628");
    BitControl.PlanFragment fragment = BitControl.PlanFragment.parseFrom(new FileInputStream(file));
    List<Runnable> threads = new ArrayList<Runnable>();
    for (int i = 0; i < 20; i++) {
      threads.add(new StartFragmentThread(fragment));
    }
    for (int i = 0; i < 20; i++) {
      threads.get(i).run();
    }
  }

  @Test
  public void testLaggingFragment() throws Throwable {
    new TestConstantFolding.SmallFileCreator(folder).createFiles(1, 100_000, "csv");
    test("alter session set `planner.slice_target` = 1");
//    String slowQuery = "select test_debugging_function_wait(columns[0]) from dfs.`/Users/jaltekruse/test_data_drill/bunch_o_csv`";
//    String slowQuery = "select test_debugging_function_wait(columns[0]) from dfs.`" + folder.getRoot().toPath() +"/bigfile` order by columns[2]";
    String slowQuery = "select columns[0] from dfs.`" + folder.getRoot().toPath() +"/bigfile` order by columns[2]";
    String query = normalizeQuery(slowQuery);
    List<SilentListener> listeners = new ArrayList();
    int numQueries = 50;
    try {
      // for (int i = 0; i < numQueries; i++) {
      //   final SilentListener listener = new SilentListener();
      //   listeners.add(listener);
      //   client.runQuery(UserBitShared.QueryType.SQL, query, listener);
      //   Thread.sleep(1000);
      // }

      List<Integer> tpchToSkip = Lists.newArrayList(2,11, 15,16, 17, 19);
      for (int j = 1; j < 5; j++) {
        for (int i = 1; i < 21; i++) {
          if (tpchToSkip.contains(i)) {
            continue;
          }
          final SilentListener listener = new SilentListener();
          listeners.add(listener);
          query = normalizeQuery(getFile("queries/tpch/" + String.format("%02d", i) + ".sql")).replace(';',' ');
          client.runQuery(UserBitShared.QueryType.SQL, query, listener);
          Thread.sleep(100);
        }
      }
//      testSetupLaggingFragment();
      for (int i = 0; i < listeners.size(); i++) {
        listeners.get(i).waitForCompletion();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}