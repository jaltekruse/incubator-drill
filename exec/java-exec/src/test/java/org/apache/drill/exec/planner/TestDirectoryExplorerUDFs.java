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
package org.apache.drill.exec.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.fn.interp.TestConstantFolding;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

public class TestDirectoryExplorerUDFs extends PlanTestBase {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private class ConstantFoldingTestConfig {
    String funcName;
    String expectedFolderName;
    public ConstantFoldingTestConfig(String funcName, String expectedFolderName) {
      this.funcName = funcName;
      this.expectedFolderName = expectedFolderName;
    }
  }

  @Test
  public void testConstExprFolding_maxDir0() throws Exception {

    new TestConstantFolding.SmallFileCreator(folder).createFiles(1, 1000);

    String path = folder.getRoot().toPath().toString();
    JsonStringArrayList list = new JsonStringArrayList();

    list.add(new Text("1"));
    list.add(new Text("2"));
    list.add(new Text("3"));

    test("use dfs.root");

    List<ConstantFoldingTestConfig> tests = ImmutableList.<ConstantFoldingTestConfig>builder()
        .add(new ConstantFoldingTestConfig("maxdir", "smallfile"))
        .add(new ConstantFoldingTestConfig("imaxdir", "SMALLFILE"))
        .add(new ConstantFoldingTestConfig("mindir", "bigfile"))
        .add(new ConstantFoldingTestConfig("imindir", "BIGFILE"))
        .build();

    List<String> allFiles = ImmutableList.<String>builder()
        .add("smallfile")
        .add("SMALLFILE")
        .add("bigfile")
        .add("BIGFILE")
        .build();

    String query = "select * from dfs.`" + path + "/*/*.csv` where dir0 = %s('dfs.root','" + path + "')";
    for (ConstantFoldingTestConfig config : tests) {
      List excludedPatterns = Lists.newArrayList();
      excludedPatterns.addAll(allFiles);
      excludedPatterns.remove(config.expectedFolderName);

      testPlanMatchingPatterns(
          String.format(query, config.funcName),
          new String[] {config.expectedFolderName},
          (String[]) excludedPatterns.toArray());
    }

    testBuilder()
        .sqlQuery(String.format(query, tests.get(0).funcName))
        .unOrdered()
        .baselineColumns("columns", "dir0")
        .baselineValues(list, "smallfile")
        .go();
  }

}
