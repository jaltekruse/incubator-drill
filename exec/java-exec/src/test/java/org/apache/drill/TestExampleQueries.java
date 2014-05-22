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

import org.apache.drill.common.util.FileUtils;
import org.junit.Test;

public class TestExampleQueries extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExampleQueries.class);

  @Test
  public void testQ() throws Exception {
//    test("select count(l_shipdate) from cp.`tpch/lineitem.parquet` where l_shipdate between '1997-01-01' and '1998-01-01'");
//    test("select count(1) from cp.`tpch/lineitem.parquet` where l_partkey > 0");
//    test("select count(1) from dfs.`/tmp/1_7_0.parquet` where l_orderkey > 0");
//    test("select l_orderkey, l_extendedprice from dfs.`/tmp/1_7_0.parquet` limit 10");
//    test("select a from dfs.`/tmp/t1` where a > 0");
//    test("use dfs.tmp");
//    test("create table t12 as select * from data");
//    test("select a from t4 where a IS NOT NULL");
    test("use dfs.tmp");
//    test("create table lineitem_null_dict as select * from `part-m-00034.parquet` limit 100000");
//    test("select * from `part-m-00034.parquet` limit 100000");
//    test("alter session set `store.parquet.use_new_reader` = false");
    test("select l_orderkey, cast(l_returnflag as VARCHAR(100)), cast(l_linestatus as varchar(100)), cast(l_shipdate as varchar(100)) from lineitem_null_dict where l_orderkey > 0 limit 100");
  }

  @Test // see DRILL-553
  public void testQueryWithNullValues() throws Exception {
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
  }

  @Test
  public void testCaseReturnValueVarChar() throws Exception{
    test("select case when employee_id < 1000 then 'ABC' else 'DEF' end from cp.`employee.json` limit 5");
  }

  @Test
  public void testCaseReturnValueBigInt() throws Exception{
    test("select case when employee_id < 1000 then 1000 else 2000 end from cp.`employee.json` limit 5" );
  }

  @Test
  public void testHashPartitionSV2 () throws Exception{
    test("select count(n_nationkey) from cp.`tpch/nation.parquet` where n_nationkey > 8 group by n_regionkey");
  }

  @Test
  public void testHashPartitionSV4 () throws Exception{
    test("select count(n_nationkey) as cnt from cp.`tpch/nation.parquet` group by n_regionkey order by cnt");
  }

  @Test
  public void testSelectWithLimit() throws Exception{
    test("select employee_id,  first_name, last_name from cp.`employee.json` limit 5 ");
  }

  @Test
  public void testSelectWithLimit2() throws Exception{
    test("select l_comment, l_orderkey from cp.`tpch/lineitem.parquet` limit 10000 ");
  }

  @Test
  public void testSVRV4() throws Exception{
    test("select employee_id,  first_name from cp.`employee.json` order by employee_id ");
  }

  @Test
  public void testSVRV4MultBatch() throws Exception{
    test("select l_orderkey from cp.`tpch/lineitem.parquet` order by l_orderkey limit 10000 ");
  }

  @Test
  public void testSVRV4Join() throws Exception{
    test("select count(*) from cp.`tpch/lineitem.parquet` l, cp.`tpch/partsupp.parquet` ps \n" +
        " where l.l_partkey = ps.ps_partkey and l.l_suppkey = ps.ps_suppkey ;");
  }

  @Test
  public void testText() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/regions.csv").toURI().toString();
    String query = String.format("select * from dfs.`%s`", root);
    test(query);
  }

  @Test
  public void testTextPartitions() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/").toURI().toString();
    String query = String.format("select * from dfs.`%s`", root);
    test(query);
  }

  @Test
  public void testJoin() throws Exception{
    test("SELECT\n" +
        "  nations.N_NAME,\n" +
        "  regions.R_NAME\n" +
        "FROM\n" +
        "  dfs.`[WORKING_PATH]/../../sample-data/nation.parquet` nations\n" +
        "JOIN\n" +
        "  dfs.`[WORKING_PATH]/../../sample-data/region.parquet` regions\n" +
        "  on nations.N_REGIONKEY = regions.R_REGIONKEY");
  }


  @Test
  public void testWhere() throws Exception{
    test("select * from cp.`employee.json` ");
  }

  @Test
  public void testGroupBy() throws Exception{
    test("select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status");
  }

  @Test
  public void testExplainPhysical() throws Exception{
    test("explain plan for select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status");
  }

  @Test
  public void testExplainLogical() throws Exception{
    test("explain plan without implementation for select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status");
  }

  @Test
  public void testGroupScanRowCountExp1() throws Exception {
    test("EXPLAIN plan for select count(n_nationkey) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testGroupScanRowCount1() throws Exception {
    test("select count(n_nationkey) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testColunValueCnt() throws Exception {
    test("select count( 1 + 2) from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testGroupScanRowCountExp2() throws Exception {
    test("EXPLAIN plan for select count(*) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testGroupScanRowCount2() throws Exception {
    test("select count(*) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` where 1 < 2");
  }

}
