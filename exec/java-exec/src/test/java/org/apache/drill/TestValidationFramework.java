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
package org.apache.drill;

import org.apache.drill.exec.proto.UserBitShared;
import org.junit.Test;

/**
 * Tests written to verify that the test framework is functioning properly.
 *
 * This indirectly tests some minimal Drill functionality including CSV and JSON readers.
 */
public class TestValidationFramework extends BaseTestQuery {

  @Test
  public void generateNewBaselines() throws Exception {
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select employee_id,  first_name, last_name from cp.`employee.json` order by employee_id limit 5 ","exec/java-exec/src/test/resources/TestAltSortQueries.testSelectWithLimit.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n1.n_regionkey from cp.`tpch/nation.parquet` n1, (select n_nationkey from cp.`tpch/nation.parquet`) as n2 where n1.n_nationkey = n2.n_nationkey","exec/java-exec/src/test/resources/TestBugFixes.DRILL883.tsv");
//    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select count(*) from cp.`tpch/nation.parquet` n left outer join cp.`tpch/region.parquet` r on n.n_regionkey = r.r_regionkey and n.n_nationkey > 10","exec/java-exec/src/test/resources/TestBugFixes.testDRILL1337_LocalLeftFilterLeftOutJoin.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`tpch/nation.parquet` n left outer join cp.`tpch/region.parquet` r on n.n_regionkey = r.r_regionkey and r.r_name not like '%ASIA' order by r.r_name","exec/java-exec/src/test/resources/TestBugFixes.testDRILL1337_LocalRightFilterLeftOutJoin.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select case when employee_id < 1000 then 1000 else 2000 end from cp.`employee.json` limit 5","exec/java-exec/src/test/resources/TestExampleQueries.testCaseReturnValueBigInt.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select case when employee_id < 1000 then 'ABC' else 'DEF' end from cp.`employee.json` limit 5","exec/java-exec/src/test/resources/TestExampleQueries.testCaseReturnValueVarChar.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select count( 1 + 2) from cp.`tpch/nation.parquet` ","exec/java-exec/src/test/resources/TestExampleQueries.testColunValueCnt.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select count(n_nationkey) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ","exec/java-exec/src/test/resources/TestExampleQueries.testGroupScanRowCount1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select count(*) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` where 1 < 2","exec/java-exec/src/test/resources/TestExampleQueries.testGroupScanRowCount2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"EXPLAIN plan for select count(n_nationkey) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ","exec/java-exec/src/test/resources/TestExampleQueries.testGroupScanRowCountExp1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"EXPLAIN plan for select count(*) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ","exec/java-exec/src/test/resources/TestExampleQueries.testGroupScanRowCountExp2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select count(n_nationkey) from cp.`tpch/nation.parquet` where n_nationkey > 8 group by n_regionkey","exec/java-exec/src/test/resources/TestExampleQueries.testHashPartitionSV2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select count(n_nationkey) as cnt from cp.`tpch/nation.parquet` group by n_regionkey order by cnt","exec/java-exec/src/test/resources/TestExampleQueries.testHashPartitionSV4.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select a.n_nationkey from cp.`tpch/nation.parquet` a join cp.`tpch/region.parquet` b on a.n_regionkey + 1 = b.r_regionkey and a.n_regionkey + 1 = b.r_regionkey","exec/java-exec/src/test/resources/TestExampleQueries.testJoinExpOn.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select a.n_nationkey from cp.`tpch/nation.parquet` a , cp.`tpch/region.parquet` b where a.n_regionkey + 1 = b.r_regionkey and a.n_regionkey + 1 = b.r_regionkey","exec/java-exec/src/test/resources/TestExampleQueries.testJoinExpWhere.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n_nationkey, n_name from cp.`tpch/nation.parquet` limit 0","exec/java-exec/src/test/resources/TestExampleQueries.testLimit0_1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n_nationkey, n_name from cp.`tpch/nation.parquet` limit 0 offset 5","exec/java-exec/src/test/resources/TestExampleQueries.testLimit0_1_1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n_nationkey, n_name from cp.`tpch/nation.parquet` order by n_nationkey limit 0","exec/java-exec/src/test/resources/TestExampleQueries.testLimit0_1_2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`tpch/nation.parquet` limit 0","exec/java-exec/src/test/resources/TestExampleQueries.testLimit0_1_3.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n.n_nationkey from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey limit 0","exec/java-exec/src/test/resources/TestExampleQueries.testLimit0_1_4.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n_regionkey, count(*) from cp.`tpch/nation.parquet` group by n_regionkey limit 0","exec/java-exec/src/test/resources/TestExampleQueries.testLimit0_1_5.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select r_name from cp.`tpch/region.parquet` order by r_regionkey","exec/java-exec/src/test/resources/TestExampleQueries.testOrderByDiffColumn.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select r_name from cp.`tpch/region.parquet` order by r_name, r_regionkey","exec/java-exec/src/test/resources/TestExampleQueries.testOrderByDiffColumn_1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select recipe from cp.`parquet/complex.parquet`","exec/java-exec/src/test/resources/TestExampleQueries.testParquetComplex.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`parquet/complex.parquet`","exec/java-exec/src/test/resources/TestExampleQueries.testParquetComplex_1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select recipe, c.inventor.name as name, c.inventor.age as age from cp.`parquet/complex.parquet` c","exec/java-exec/src/test/resources/TestExampleQueries.testParquetComplex_2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select count(*) from cp.`customer.json` limit 1","exec/java-exec/src/test/resources/TestExampleQueries.testQueryWithNullValues.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select employee_id,  first_name, last_name from cp.`employee.json` limit 5 ","exec/java-exec/src/test/resources/TestExampleQueries.testSelectWithLimit.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select l_comment, l_orderkey from cp.`tpch/lineitem.parquet` limit 10000 ","exec/java-exec/src/test/resources/TestExampleQueries.testSelectWithLimit2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n.* from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey order by n.n_name","exec/java-exec/src/test/resources/TestExampleQueries.testSelLeftStarJoin.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select r.* from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey order by n.n_name","exec/java-exec/src/test/resources/TestExampleQueries.testSelRightStarJoin.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n.*, r.* from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarBothSideJoin.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey order by n.n_name","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarJoin.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`tpch/nation.parquet` n1, cp.`tpch/nation.parquet` n2 where n1.n_nationkey = n2.n_nationkey","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarJoinSameColName.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select t1.name, t1.kind, t2.n_nationkey from (select * from sys.options) t1 join (select * from cp.`tpch/nation.parquet`) t2 on t1.name = t2.n_name","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarJoinSchemaWithSchemaLess.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`employee.json` order by last_name","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarOrderBy.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`employee.json` order by employee_id limit 2","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarOrderByLimit.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select *, n_nationkey from cp.`tpch/nation.parquet` limit 2","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarPlusRegCol.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select *, n.n_nationkey, 1 + 2 as constant from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey order by n.n_name","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarRegColConstJoin.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select v.first_name from (select * from cp.`employee.json`) v limit 2","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarSubQJson2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n_nationkey, n_name, n_regionkey from (select * from cp.`tpch/nation.parquet`)  where n_regionkey > 1 order by n_name","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarSubQNoPrefix.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n_regionkey, count(*) as cnt from (select * from cp.`tpch/nation.parquet`) t where n_nationkey > 1 group by n_regionkey order by n_regionkey","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarSubQNoPrefix_1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select t.n_nationkey, t.n_name, t.n_regionkey from (select * from cp.`tpch/nation.parquet`) t where t.n_regionkey > 1 order by t.n_name","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarSubQPrefix.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select t.n_regionkey, count(*) as cnt from (select * from cp.`tpch/nation.parquet`) t where t.n_nationkey > 1 group by t.n_regionkey order by t.n_regionkey","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarSubQPrefix_1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select name, kind, type from (select * from sys.options)","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarSubQSchemaTable.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`employee.json` where first_name = 'James' order by employee_id","exec/java-exec/src/test/resources/TestExampleQueries.testSelStarWhereOrderBy.tsv");
//    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from vt1","exec/java-exec/src/test/resources/TestExampleQueries.testStarView1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select employee_id,  first_name from cp.`employee.json` order by employee_id ","exec/java-exec/src/test/resources/TestExampleQueries.testSVRV4.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select l_orderkey from cp.`tpch/lineitem.parquet` order by l_orderkey limit 10000 ","exec/java-exec/src/test/resources/TestExampleQueries.testSVRV4MultBatch.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`employee.json` ","exec/java-exec/src/test/resources/TestExampleQueries.testWhere.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n_regionkey from cp.`tpch/nation.parquet` union all select r_regionkey from cp.`tpch/region.parquet`","exec/java-exec/src/test/resources/TestUnionAll.testUnionAll1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`tpch/region.parquet` r1 union all select * from cp.`tpch/region.parquet` r2","exec/java-exec/src/test/resources/TestUnionAll.testUnionAll5.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` where n_regionkey = 1 union all select r_regionkey, r_regionkey from cp.`tpch/region.parquet` where r_regionkey = 2","exec/java-exec/src/test/resources/TestUnionAll.testUnionAll6.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n_nationkey, n_nationkey from cp.`tpch/nation.parquet` union all select r_regionkey, r_regionkey from cp.`tpch/region.parquet`","exec/java-exec/src/test/resources/TestUnionAll.testUnionAll6_1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select 'abc' from cp.`tpch/region.parquet` union all select 'abcdefgh' from cp.`tpch/region.parquet`","exec/java-exec/src/test/resources/TestUnionAll.testUnionAll7.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select n_name from cp.`tpch/nation.parquet` union all select r_comment from cp.`tpch/region.parquet`","exec/java-exec/src/test/resources/TestUnionAll.testUnionAll8.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select tbl.arrayval[0] from cp.`nested/nested_1.json` tbl","exec/java-exec/src/test/resources/exec/nested/TestNestedComplexSchema.testNested1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select tbl.a.arrayval[0] from cp.`nested/nested_2.json` tbl","exec/java-exec/src/test/resources/exec/nested/TestNestedComplexSchema.testNested2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select tbl.a.arrayval[0].val1[0] from cp.`nested/nested_3.json` tbl","exec/java-exec/src/test/resources/exec/nested/TestNestedComplexSchema.testNested3.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from sys.drillbits","exec/java-exec/src/test/resources/exec/server/TestOptions.testDrillbits.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select cast('false' as boolean), cast('true' as boolean) from sys.options limit 1","exec/java-exec/src/test/resources/exec/sql/TestSimpleCastFunctions.castToBoolean.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select sum(position_id) over w from cp.`employee.json` window w as ( partition by position_id order by position_id)","exec/java-exec/src/test/resources/exec/sql/TestWindowFunctions.testWindowSum.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select `integer`, x['y'] as x1, x['y'] as x2, z[0], z[0]['orange'], z[1]['pink']  from cp.`jsoninput/input2.json` limit 10 ","exec/java-exec/src/test/resources/exec/store/json/JsonRecordReader2Test.testComplexJsonInput.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`join/merge_join.json`","exec/java-exec/src/test/resources/exec/store/json/JsonRecordReader2Test.testComplexMultipleTimes.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select * from cp.`limit/test1.json` limit 10","exec/java-exec/src/test/resources/exec/store/json/JsonRecordReader2Test.trySimpleQueryWithLimit.tsv");
//    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select columns[0] as region_id, columns[1] as country from dfs_test.`[WORKING_PATH]/src/test/resources/store/text/data/regions.csv`","exec/java-exec/src/test/resources/exec/store/text/TestTextColumn.testCsvColumnSelection.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select l, l from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.test_repeatedList.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"  select convert_to(types, 'JSON') from cp.`jsoninput/vvtypes.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testA0.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"  select convert_to(types[1], 'JSON') from cp.`jsoninput/vvtypes.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testA1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"  select convert_to(types[1].minor, 'JSON') from cp.`jsoninput/vvtypes.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testA2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"  select convert_to(types[1].minor[0].valueHolder, 'JSON') from cp.`jsoninput/vvtypes.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testA3.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"  select convert_to(types[1], 'JSON'), convert_to(modes[2], 'JSON') from cp.`jsoninput/vvtypes.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testA4.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"  select types[1] from cp.`jsoninput/vvtypes.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testB1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"  select types[1].minor from cp.`jsoninput/vvtypes.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testB2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"  select types[1].minor[0].valueholder from cp.`jsoninput/vvtypes.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testB3.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select convert_to(z[0], 'JSON') from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testX.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select convert_to(x, 'JSON') from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testX2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select convert_to(tbl.x.y, 'JSON') from cp.`jsoninput/input2.json` tbl","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testX3.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select convert_to(`float`, 'JSON') from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testX4.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select convert_to(`integer`, 'JSON') from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testX5.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select convert_to(z, 'JSON')  from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testX6.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select convert_to(rl[1], 'JSON') from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testX7.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select convert_to(rl[0][1], 'JSON') from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testX8.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select convert_to(rl, 'JSON') from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testX9.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select z[0] from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testY.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select x from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testY2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select tbl.x.y from cp.`jsoninput/input2.json` tbl","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testY3.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select z  from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testY6.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select rl[1] from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testZ.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select rl[0][1] from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testZ1.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select rl[1000][1] from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testZ2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select rl[0][1000] from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testZ3.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL,"select rl, rl from cp.`jsoninput/input2.json`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeReader.testZ4.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL," select convert_from('{x:100, y:215.6}' ,'JSON') as mycol from cp.`tpch/nation.parquet`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeWriter.testA0.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL," select convert_from('{x:100, y:215.6}' ,'JSON') as mycol1, convert_from('{x:100, y:215.6}' ,'JSON') as mycol2 from cp.`tpch/nation.parquet`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeWriter.testA2.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL," select convert_from(concat('{x:100,',  'y:215.6}') ,'JSON') as mycol1, convert_from('{x:100, y:215.6}' ,'JSON') as mycol2 from cp.`tpch/nation.parquet`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeWriter.testA3.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL," select convert_from('{}' ,'JSON') as mycol1, convert_from('{x:100, y:215.6}' ,'JSON') as mycol2 from cp.`tpch/nation.parquet`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeWriter.testA4.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL," select convert_from('[]' ,'JSON') as mycol1, convert_from('{x:100, y:215.6}' ,'JSON') as mycol2 from cp.`tpch/nation.parquet`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeWriter.testA5.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL," select convert_from('[1, 2, 3]' ,'JSON') as mycol1  from cp.`tpch/nation.parquet`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeWriter.testA6.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL," select convert_from('[1.2, 2.3, 3.5]' ,'JSON') as mycol1  from cp.`tpch/nation.parquet`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeWriter.testA7.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL," select convert_from('[ [1, 2], [3, 4], [5]]' ,'JSON') as mycol1  from cp.`tpch/nation.parquet`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeWriter.testA8.tsv");
    testRunAndWriteToFile(UserBitShared.QueryType.SQL," select convert_from('[{a : 100, b: 200}, {a:300, b: 400}]' ,'JSON') as mycol1  from cp.`tpch/nation.parquet`","exec/java-exec/src/test/resources/exec/vector/complex/writer/TestComplexTypeWriter.testA9.tsv");

  }

}
