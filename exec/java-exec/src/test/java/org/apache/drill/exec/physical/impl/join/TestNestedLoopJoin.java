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

package org.apache.drill.exec.physical.impl.join;

import org.apache.drill.PlanTestBase;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.junit.Ignore;
import org.junit.Test;

public class TestNestedLoopJoin extends PlanTestBase {

  private static String nlpattern = "NestedLoopJoin";
  private static final String WORKING_PATH = TestTools.getWorkingPath();
  private static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  // Test queries used by planning and execution tests
  private static final String testNlJoinExists_1 = "select r_regionkey from cp.`tpch/region.parquet` "
      + " where exists (select n_regionkey from cp.`tpch/nation.parquet` "
      + " where n_nationkey < 10)";

  private static final String testNlJoinNotIn_1 = "select r_regionkey from cp.`tpch/region.parquet` "
      + " where r_regionkey not in (select n_regionkey from cp.`tpch/nation.parquet` "
      + "                            where n_nationkey < 4)";

  // not-in subquery produces empty set
  private static final String testNlJoinNotIn_2 = "select r_regionkey from cp.`tpch/region.parquet` "
      + " where r_regionkey not in (select n_regionkey from cp.`tpch/nation.parquet` "
      + "                            where 1=0)";

  private static final String testNlJoinInequality_1 = "select r_regionkey from cp.`tpch/region.parquet` "
      + " where r_regionkey > (select min(n_regionkey) from cp.`tpch/nation.parquet` "
      + "                        where n_nationkey < 4)";

  private static final String testNlJoinInequality_2 = "select r.r_regionkey, n.n_nationkey from cp.`tpch/nation.parquet` n "
      + " inner join cp.`tpch/region.parquet` r on n.n_regionkey < r.r_regionkey where n.n_nationkey < 3";

  private static final String testNlJoinInequality_3 = "select r_regionkey from cp.`tpch/region.parquet` "
      + " where r_regionkey > (select min(n_regionkey) * 2 from cp.`tpch/nation.parquet` )";


  @Test
  public void testNlJoinExists_1_planning() throws Exception {
    testPlanMatchingPatterns(testNlJoinExists_1, new String[]{nlpattern}, new String[]{});
  }

  @Test
  // @Ignore
  public void testNlJoinNotIn_1_planning() throws Exception {
    testPlanMatchingPatterns(testNlJoinNotIn_1, new String[]{nlpattern}, new String[]{});
  }

  @Test
  public void testNlJoinInequality_1() throws Exception {
    testPlanMatchingPatterns(testNlJoinInequality_1, new String[]{nlpattern}, new String[]{});
  }

  @Test
  public void testNlJoinInequality_2() throws Exception {
    try {
      setOption(PlannerSettings.NLJOIN_FOR_SCALAR, false);
      testPlanMatchingPatterns(testNlJoinInequality_2, new String[]{nlpattern}, new String[]{});
    } finally {
      resetOption(PlannerSettings.NLJOIN_FOR_SCALAR);
    }
  }

  @Test
  @Ignore // Re-test after CALCITE-695 is resolved
  public void testNlJoinInequality_3() throws Exception {
    try {
      setOption(PlannerSettings.NLJOIN_FOR_SCALAR, false);
      testPlanMatchingPatterns(testNlJoinInequality_3, new String[]{nlpattern}, new String[]{});
    } finally {
      resetOption(PlannerSettings.NLJOIN_FOR_SCALAR);
    }
  }

  @Test
  public void testNlJoinAggrs_1_planning() throws Exception {
    String query = "select total1, total2 from "
       + "(select sum(l_quantity) as total1 from cp.`tpch/lineitem.parquet` where l_suppkey between 100 and 200), "
       + "(select sum(l_quantity) as total2 from cp.`tpch/lineitem.parquet` where l_suppkey between 200 and 300)  ";
    testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
  }

  @Test // equality join and scalar right input, hj and mj disabled
  public void testNlJoinEqualityScalar_1_planning() throws Exception {
    String query = "select r_regionkey from cp.`tpch/region.parquet` "
        + " where r_regionkey = (select min(n_regionkey) from cp.`tpch/nation.parquet` "
        + "                        where n_nationkey < 10)";
    try {
      setOption(PlannerSettings.HASHJOIN, false);
      setOption(PlannerSettings.MERGEJOIN, false);
      testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
    } finally {
      resetOptions(PlannerSettings.HASHJOIN, PlannerSettings.MERGEJOIN);
    }
  }

  @Test // equality join and scalar right input, hj and mj disabled, enforce exchanges
  public void testNlJoinEqualityScalar_2_planning() throws Exception {
    String query = "select r_regionkey from cp.`tpch/region.parquet` "
        + " where r_regionkey = (select min(n_regionkey) from cp.`tpch/nation.parquet` "
        + "                        where n_nationkey < 10)";
    try {
      setOption(ExecConstants.PLANNER_SLICE_TARGET, 1);
      setOption(PlannerSettings.HASHJOIN, false);
      setOption(PlannerSettings.MERGEJOIN, false);
      testPlanMatchingPatterns(query, new String[]{nlpattern, "BroadcastExchange"}, new String[]{});
    } finally {
      resetOptions(ExecConstants.PLANNER_SLICE_TARGET, PlannerSettings.HASHJOIN, PlannerSettings.MERGEJOIN);
    }
  }

  @Test // equality join and non-scalar right input, hj and mj disabled
  public void testNlJoinEqualityNonScalar_1_planning() throws Exception {
    String query = "select r.r_regionkey from cp.`tpch/region.parquet` r inner join cp.`tpch/nation.parquet` n"
        + " on r.r_regionkey = n.n_regionkey where n.n_nationkey < 10";
    try {
      setAllFalse(PlannerSettings.HASHJOIN, PlannerSettings.MERGEJOIN, PlannerSettings.NLJOIN_FOR_SCALAR);
      testPlanMatchingPatterns(query, new String[]{nlpattern}, new String[]{});
    } finally {
      setAllTrue(PlannerSettings.HASHJOIN, PlannerSettings.MERGEJOIN, PlannerSettings.NLJOIN_FOR_SCALAR);
    }
  }

  @Test // equality join and non-scalar right input, hj and mj disabled, enforce exchanges
  public void testNlJoinEqualityNonScalar_2_planning() throws Exception {
    String query = String.format("select n.n_nationkey from cp.`tpch/nation.parquet` n, "
        + " dfs_test.`%s/multilevel/parquet` o "
        + " where n.n_regionkey = o.o_orderkey and o.o_custkey < 5", TEST_RES_PATH);
    try {
      setAllFalse(PlannerSettings.HASHJOIN, PlannerSettings.MERGEJOIN, PlannerSettings.NLJOIN_FOR_SCALAR);
      setOption(ExecConstants.PLANNER_SLICE_TARGET, 1);
      testPlanMatchingPatterns(query, new String[]{nlpattern, "BroadcastExchange"}, new String[]{});
    } finally {
      setAllTrue(PlannerSettings.HASHJOIN, PlannerSettings.MERGEJOIN, PlannerSettings.NLJOIN_FOR_SCALAR);
      resetToDefaultExchanges();
    }
  }

  // EXECUTION TESTS

  @Test
  public void testNlJoinExists_1_exec() throws Exception {
    testBuilder()
        .sqlQuery(testNlJoinExists_1)
        .unOrdered()
        .baselineColumns("r_regionkey")
        .baselineValues(0)
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .go();
  }

  @Test
  public void testNlJoinNotIn_1_exec() throws Exception {
    testBuilder()
        .sqlQuery(testNlJoinNotIn_1)
        .unOrdered()
        .baselineColumns("r_regionkey")
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .go();
  }

  @Test
  public void testNlJoinNotIn_2_exec() throws Exception {
    testBuilder()
        .sqlQuery(testNlJoinNotIn_2)
        .unOrdered()
        .baselineColumns("r_regionkey")
        .baselineValues(0)
        .baselineValues(1)
        .baselineValues(2)
        .baselineValues(3)
        .baselineValues(4)
        .go();
  }

  @Test
  public void testNLJWithEmptyBatch() throws Exception {
    Long result = 0l;

    try {
      setAllFalse(PlannerSettings.HASHJOIN, PlannerSettings.MERGEJOIN, PlannerSettings.NLJOIN_FOR_SCALAR);

      // We have a false filter causing empty left batch
      String query = "select count(*) col from (select a.lastname " +
          "from cp.`employee.json` a " +
          "where exists (select n_name from cp.`tpch/nation.parquet` b) AND 1 = 0)";

      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("col")
          .baselineValues(result)
          .go();

      // Below tests use NLJ in a general case (non-scalar subqueries, followed by filter) with empty batches
      query = "select count(*) col from " +
          "(select t1.department_id " +
          "from cp.`employee.json` t1 inner join cp.`department.json` t2 " +
          "on t1.department_id = t2.department_id where t1.department_id = -1)";

      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("col")
          .baselineValues(result)
          .go();

      query = "select count(*) col from " +
          "(select t1.department_id " +
          "from cp.`employee.json` t1 inner join cp.`department.json` t2 " +
          "on t1.department_id = t2.department_id where t2.department_id = -1)";


      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("col")
          .baselineValues(result)
          .go();
    } finally {
      resetOptions(PlannerSettings.HASHJOIN, PlannerSettings.MERGEJOIN, PlannerSettings.NLJOIN_FOR_SCALAR);
    }
  }
}
