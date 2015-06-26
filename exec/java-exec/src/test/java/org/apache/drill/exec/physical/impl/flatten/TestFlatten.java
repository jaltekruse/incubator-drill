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
package org.apache.drill.exec.physical.impl.flatten;

import static org.apache.drill.TestBuilder.listOf;
import static org.apache.drill.TestBuilder.mapOf;
import static org.junit.Assert.assertEquals;

import com.beust.jcommander.internal.Lists;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.TestBuilder;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.fn.interp.TestConstantFolding;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.hadoop.io.Text;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

public class TestFlatten extends BaseTestQuery {

  /**
   *  enable this if you have the following files:
   *    - /tmp/yelp_academic_dataset_business.json
   *    - /tmp/mapkv.json
   *    - /tmp/drill1665.json
   *    - /tmp/bigfile.json
   */
  public static boolean RUN_ADVANCED_TESTS = false;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testFlattenFailure() throws Exception {
    test("select flatten(complex), rownum from cp.`/store/json/test_flatten_mappify2.json`");
//    test("select complex, rownum from cp.`/store/json/test_flatten_mappify2.json`");
  }

  @Test
  public void testProjectCreatedByFlattenPlanningRule() throws Exception {
    // This query produces extra copies of complex columns, while they are not specifically needed in this case these
    // types of extra copies are supported and were previously causing query failures. These codepaths will become
    // more common as we expand the support in Drill for more functions an operations to manipulate complex data.
    testBuilder()
        .sqlQuery("select e2 as events, e2 as events2, uid from (select v1.uid, v1.event_map.events as e, v1.event_map.events as e2 from " +
        "    (select uid, event_map from cp.`flatten/complex_transaction_example_data_modified.json`) v1 " +
        "inner join " +
        "    (select uid, transaction_map from cp.`flatten/complex_transaction_example_data_modified.json`) v2 " +
        "on v1.uid = v2.uid)")
        .unOrdered()
        .jsonBaselineFile("flatten/complex_transaction_repeated_map_double_copy_baseline.json")
        .go();
  }

  @Test
  public void testProjectCreatedByFlattenPlanningRule2() throws Exception {
    // same test as above but removing a level of indirection in the schema
//    sqlQuery("select e2 as events, e2 as events2, uid from (select v1.uid, events as e, events as e2 from \n" +
//       "    (select uid, events from cp.`flatten/complex_transaction_example_data.json`) v1\n" +
//       "inner join\n" +
//       "    (select uid, transactions from cp.`flatten/complex_transaction_example_data.json`) v2\n" +
//       "on v1.uid = v2.uid)");


    testBuilder()
        .sqlQuery("select e2 as events, e2 as events2, uid from (select v1.uid, events as e, events as e2 from \n" +
        "    (select uid, events from cp.`flatten/complex_transaction_example_data.json`) v1\n" +
        "inner join\n" +
        "    (select uid, transactions from cp.`flatten/complex_transaction_example_data.json`) v2\n" +
        "on v1.uid = v2.uid)")
//        .sqlQuery("select e2 as events, e2 as events2, uid from (select v1.uid, v1.event_map.events as e, v1.event_map.events as e2 from " +
//            "    (select uid, event_map from cp.`flatten/complex_transaction_example_data_modified.json`) v1 " +
//            "inner join " +
//            "    (select uid, transaction_map from cp.`flatten/complex_transaction_example_data_modified.json`) v2 " +
//            "on v1.uid = v2.uid)")
        .unOrdered()
        .jsonBaselineFile("flatten/complex_transaction_repeated_map_double_copy_baseline.json")
        .go();
  }

  @Test
  public void testFlattenWithJoinNested() throws Exception {
    test("select v1.uid, flatten(v1.event_map.events), flatten(v2.transaction_map.transactions) from \n" +
        "    (select uid, event_map from cp.`flatten/complex_transaction_example_data_modified.json`) v1\n" +
        "inner join\n" +
        "    (select uid, transaction_map from cp.`flatten/complex_transaction_example_data_modified.json`) v2\n" +
        "on v1.uid = v2.uid;");
  }

  @Test
  public void testFlattenWithJoin() throws Exception {
    test("alter session set `planner.width.max_per_node` = 1");
    test("select v1.uid, flatten(events), flatten(transactions) from \n" +
        "    (select uid, events from cp.`flatten/complex_transaction_example_data.json`) v1\n" +
        "inner join\n" +
        "    (select uid, transactions from cp.`flatten/complex_transaction_example_data.json`) v2\n" +
        "on v1.uid = v2.uid;");

    // both of these work
//    test("select e as events2, e as e2 from (select uid as u, events as e, events from cp.`flatten/complex_transaction_example_data.json`)");
//    test("select events as events2, events as e2 from (select uid as u, events as e, events from cp.`flatten/complex_transaction_example_data.json`)");

//   test("select uid, transactions from (select uid, transactions from cp.`flatten/complex_transaction_example_data.json`) as t");
//   testPhysicalFromFile("flatten/test_project_complex_twice_plan.json");
  }

  @Test
  public void testFlattenDatasetFromList() throws Exception {
    String path = folder.getRoot().toPath().toString();

    String jsonRecords = BaseTestQuery.getFile("flatten/FROM_LIST_ASK_PERMISSION_TO_DISTRIBUTE.json");
    int numCopies = 10000;
    new TestConstantFolding.SmallFileCreator(folder)
        .setRecord(jsonRecords)
        .createFiles(1, numCopies, "json");

//    test("select flatten(campaign['funders'])['user_id']  from " +
    test("select flatten(d.campaign.funders)['user_id']  from " +
                "dfs.`" + path + "/bigfile/bigfile.json` d");
  }

  @Test
  public void testFlatten_Drill2162_complex() throws Exception {
    String path = folder.getRoot().toPath().toString();

    String jsonRecords = BaseTestQuery.getFile("flatten/complex_transaction_example_data.json");
    int numCopies = 700;
    new TestConstantFolding.SmallFileCreator(folder)
        .setRecord(jsonRecords)
        .createFiles(1, numCopies, "json");

    List<JsonStringHashMap<String,Object>> data = Lists.newArrayList(
        mapOf("uid", 1l,
            "lst_lst_0", listOf(1l, 2l, 3l, 4l, 5l),
            "lst_lst_1", listOf(2l, 3l, 4l, 5l, 6l),
            "lst_lst",listOf(
                listOf(1l, 2l, 3l, 4l, 5l),
                listOf(2l, 3l, 4l, 5l, 6l))
        ),
        mapOf("uid", 2l,
            "lst_lst_0", listOf(1l, 2l, 3l, 4l, 5l),
            "lst_lst_1", listOf(2l, 3l, 4l, 5l, 6l),
            "lst_lst",listOf(
                listOf(1l, 2l, 3l, 4l, 5l),
                listOf(2l, 3l, 4l, 5l, 6l))
        )
    );

    List<JsonStringHashMap<String, Object>> result = flatten(flatten(flatten(data, "lst_lst_1"), "lst_lst_0"), "lst_lst");

    TestBuilder builder = testBuilder()
        .sqlQuery("select uid, flatten(d.lst_lst[1]) lst1, flatten(d.lst_lst[0]) lst0, flatten(d.lst_lst) lst from " +
                  "dfs.`" + path + "/bigfile/bigfile.json` d")
        .unOrdered()
        .baselineColumns("uid", "lst1", "lst0", "lst");
    for (int i = 0; i < numCopies; i++) {
      for (JsonStringHashMap<String, Object> record : result) {
        builder.baselineValues(record.get("uid"), record.get("lst_lst_1"), record.get("lst_lst_0"), record.get("lst_lst"));
      }
    }
    builder.go();
  };

  @Test
  public void testFlattenReferenceImpl() throws Exception {
    List<JsonStringHashMap<String,Object>> data = Lists.newArrayList(
        mapOf("a",1,
              "b",2,
              "list_col", listOf(10,9),
              "nested_list_col",listOf(
                  listOf(100,99),
                  listOf(1000,999)
            )));
    List<JsonStringHashMap<String, Object>> result = flatten(flatten(flatten(data, "list_col"), "nested_list_col"), "nested_list_col");
     List<JsonStringHashMap<String, Object>> expectedResult = Lists.newArrayList(
        mapOf("nested_list_col", 100,  "list_col", 10,"a", 1, "b",2),
        mapOf("nested_list_col", 99,   "list_col", 10,"a", 1, "b",2),
        mapOf("nested_list_col", 1000, "list_col", 10,"a", 1, "b",2),
        mapOf("nested_list_col", 999,  "list_col", 10,"a", 1, "b",2),
        mapOf("nested_list_col", 100,  "list_col", 9, "a", 1, "b",2),
        mapOf("nested_list_col", 99,   "list_col", 9, "a", 1, "b",2),
        mapOf("nested_list_col", 1000, "list_col", 9, "a", 1, "b",2),
        mapOf("nested_list_col", 999,  "list_col", 9, "a", 1, "b",2)
    );
    int i = 0;
    for (JsonStringHashMap record : result) {
      assertEquals(record, expectedResult.get(i));
      i++;
    }
  }

  private List<JsonStringHashMap<String, Object>> flatten(
      List<JsonStringHashMap<String,Object>> incomingRecords,
      String colToFlatten) {
    return flatten(incomingRecords, colToFlatten, colToFlatten);
  }

  private List<JsonStringHashMap<String, Object>> flatten(
      List<JsonStringHashMap<String,Object>> incomingRecords,
      String colToFlatten,
      String flattenedDataColName) {
    List<JsonStringHashMap<String,Object>> output = Lists.newArrayList();
    for (JsonStringHashMap<String, Object> incomingRecord : incomingRecords) {
      List dataToFlatten = (List) incomingRecord.get(colToFlatten);
      for (int i = 0; i < dataToFlatten.size(); i++) {
        final JsonStringHashMap newRecord = new JsonStringHashMap();
        newRecord.put(flattenedDataColName, dataToFlatten.get(i));
        for (String s : incomingRecord.keySet()) {
          if (s.equals(colToFlatten)) {
            continue;
          }
          newRecord.put(s, incomingRecord.get(s));
        }
        output.add(newRecord);
      }
    }
    return output;
  }

  @Test
  public void testFlatten_Drill2162_simple() throws Exception {
    String path = folder.getRoot().toPath().toString();

    List<Long> inputList = Lists.newArrayList();
    String jsonRecord = "{ \"int_list\" : [";
    final int listSize = 30;
    for (int i = 1; i < listSize; i++ ) {
      jsonRecord += i + ", ";
      inputList.add((long) i);
    }
    jsonRecord += listSize + "] }";
    inputList.add((long) listSize);
    int numRecords = 3000;
    new TestConstantFolding.SmallFileCreator(folder)
        .setRecord(jsonRecord)
        .createFiles(1, numRecords, "json");

    List<JsonStringHashMap<String,Object>> data = Lists.newArrayList(
        mapOf("int_list", inputList)
    );

    List<JsonStringHashMap<String, Object>> result = flatten(data, "int_list");

    TestBuilder builder = testBuilder()
        .sqlQuery("select flatten(int_list) as int_list from dfs.`" + path + "/bigfile/bigfile.json`")
        .unOrdered()
        .baselineColumns("int_list");

    for (int i = 0; i < numRecords; i++) {
      for (JsonStringHashMap<String, Object> record : result) {
        builder.baselineValues(record.get("int_list"));
      }
    }
    builder.go();
  };

  private String generateIntList(int listSize) {
    StringBuffer jsonList = new StringBuffer("[");
    for (int i = 1; i < listSize; i++ ) {
      jsonList.append(i).append(", ");
    }
    return jsonList.append(listSize).append("]").toString();
  }

  @Test
  public void readLargeLists() throws Exception {
    String path = folder.getRoot().toPath().toString();

    final int listSize = 99_999;
    String jsonRecord = new StringBuffer("{ \"int_list\" : ").append(generateIntList(listSize)).append("}").toString();
    int numRecords = 2000;

    new TestConstantFolding.SmallFileCreator(folder)
        .setRecord(jsonRecord)
        .createFiles(1, numRecords, "json");

    runSQL("select int_list from dfs.`" + path + "/bigfile/bigfile.json`");
  };

  @Test
  public void largeList_2851() throws Exception {
    String path = folder.getRoot().toPath().toString();

    final int listSize = 99_999;
    String jsonRecord = new StringBuffer("{ \"int_list\" : ").append(generateIntList(listSize)).append("}").toString();

    int numRecords = 1;
    new TestConstantFolding.SmallFileCreator(folder)
        .setRecord(jsonRecord)
        .createFiles(1, numRecords, "json");

    assertEquals("Wrong record count returned from flatten.", listSize * numRecords,
        testRunAndPrint(UserBitShared.QueryType.SQL,
            "select flatten(int_list) from dfs.`" + path + "/bigfile/bigfile.json`"));
  };

  @Test
  public void drill1671() throws Exception{
    int rowCount = testSql("select * from (select count(*) as cnt from (select id, flatten(evnts1), flatten(evnts2), flatten(evnts3), flatten(evnts4), flatten(evnts5), flatten(evnts6), flatten(evnts7), flatten(evnts8), flatten(evnts9), flatten(evnts10), flatten(evnts11) from cp.`/flatten/many-arrays-50.json`)x )y where cnt = 2048");
    assertEquals(rowCount, 1);
  }

  @Test
  @Ignore("not yet fixed")
  public void drill1660() throws Exception {
    test("select * from cp.`/flatten/empty-rm.json`");
  }

  @Test // repeated list within a repeated map
  public void drill1673() throws Exception {
    String path = folder.getRoot().toPath().toString();

    String jsonRecords = BaseTestQuery.getFile("store/json/1673.json");
    int numCopies = 25000;
    new TestConstantFolding.SmallFileCreator(folder)
        .setRecord(jsonRecords)
        .createFiles(1, numCopies, "json");

    TestBuilder builder = testBuilder()
        .sqlQuery("select t.fixed_column as fixed_column, " +
                  "flatten(t.list_column) as list_col " +
                  "from dfs.`" + path + "/bigfile/bigfile.json` as t")
        .baselineColumns("fixed_column", "list_col")
        .unOrdered();
    Object map1 = mapOf("id1", "1",
                        "name", "zhu",
                        "num", listOf(listOf(1l, 2l, 3l)));
    Object map2 = mapOf("id1", "2",
                      "name", "hao",
                      "num", listOf(listOf(4l, 5l, 6l)));
    for (int i = 0; i < numCopies; i++) {
      builder.baselineValues("abc", map1);
      builder.baselineValues("abc", map2);
    }

    builder.go();
  }

  @Test
  public void drill1653() throws Exception{
    int rowCount = testSql("select * from (select sum(t.flat.`value`) as sm from (select id, flatten(kvgen(m)) as flat from cp.`/flatten/missing-map.json`)t) where sm = 10 ");
    assertEquals(1, rowCount);
  }

  @Test
  public void drill1652() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select uid, flatten(transactions) from dfs.`/tmp/bigfile.json`");
    }
  }

  @Test
  @Ignore("Still not working.")
  public void drill1649() throws Exception {
    test("select event_info.uid, transaction_info.trans_id, event_info.event.evnt_id\n" +
        "from (\n" +
        " select userinfo.transaction.trans_id trans_id, max(userinfo.event.event_time) max_event_time\n" +
        " from (\n" +
        "     select uid, flatten(events) event, flatten(transactions) transaction from cp.`/flatten/single-user-transactions.json`\n" +
        " ) userinfo\n" +
        " where userinfo.transaction.trans_time >= userinfo.event.event_time\n" +
        " group by userinfo.transaction.trans_id\n" +
        ") transaction_info\n" +
        "inner join\n" +
        "(\n" +
        " select uid, flatten(events) event\n" +
        " from cp.`/flatten/single-user-transactions.json`\n" +
        ") event_info\n" +
        "on transaction_info.max_event_time = event_info.event.event_time;");
  }

  @Test
  public void testKVGenFlatten1() throws Exception {
    // works - TODO and verify results
    test("select flatten(kvgen(f1)) as monkey, x " +
        "from cp.`/store/json/test_flatten_mapify.json`");
  }

  @Test
  public void testTwoFlattens() throws Exception {
    // second re-write rule has been added to test the fixes together, this now runs
    test("select `integer`, `float`, x, flatten(z), flatten(l) from cp.`/jsoninput/input2_modified.json`");
  }

  @Test
  public void testFlattenRepeatedMap() throws Exception {
    test("select `integer`, `float`, x, flatten(z) from cp.`/jsoninput/input2.json`");
  }

  @Test
  public void testFlattenKVGenFlatten() throws Exception {
    // currently does not fail, but produces incorrect results, requires second re-write rule to split up expressions
    // with complex outputs
    test("select `integer`, `float`, x, flatten(kvgen(flatten(z))) from cp.`/jsoninput/input2.json`");
  }

  @Test
  public void testKVGenFlatten2() throws Exception {
    // currently runs
    // TODO - re-verify results by hand
    if(RUN_ADVANCED_TESTS){
      test("select flatten(kvgen(visited_cellid_counts)) as mytb from dfs.`/tmp/mapkv.json`") ;
    }
  }

  @Test
  public void testFilterFlattenedRecords() throws Exception {
    // WORKS!!
    // TODO - hand verify results
    test("select t2.key from (select t.monkey.`value` as val, t.monkey.key as key from (select flatten(kvgen(f1)) as monkey, x " +
        "from cp.`/store/json/test_flatten_mapify.json`) as t) as t2 where t2.val > 1");
  }

  @Test
  public void testFilterFlattenedRecords2() throws Exception {
    // previously failed in generated code
    //  "value" is neither a method, a field, nor a member class of "org.apache.drill.exec.expr.holders.RepeatedVarCharHolder" [ 42eb1fa1-0742-4e4f-8723-609215c18900 on 10.250.0.86:31010 ]
    // appears to be resolving the data coming out of flatten as repeated, check fast schema stuff

    // FIXED BY RETURNING PROPER SCHEMA DURING FAST SCHEMA STEP
    // these types of problems are being solved more generally as we develp better support for chaning schema
    if(RUN_ADVANCED_TESTS){
      test("select celltbl.catl from (\n" +
          "        select flatten(categories) catl from dfs.`/tmp/yelp_academic_dataset_business.json` b limit 100\n" +
          "    )  celltbl where celltbl.catl = 'Doctors'");
    }
  }

  @Test
  public void countAggFlattened() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select celltbl.catl, count(celltbl.catl) from ( " +
          "select business_id, flatten(categories) catl from dfs.`/tmp/yelp_academic_dataset_business.json` b limit 100 " +
          ")  celltbl group by celltbl.catl limit 10 ");
    }
  }


  @Test
  public void flattenAndAdditionalColumn() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select business_id, flatten(categories) from dfs.`/tmp/yelp_academic_dataset_business.json` b");
    }
  }

  @Test
  public void testFailingFlattenAlone() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select flatten(categories) from dfs.`/tmp/yelp_academic_dataset_business.json` b  ");
    }
  }

  @Test
  public void testDistinctAggrFlattened() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test(" select distinct(celltbl.catl) from (\n" +
          "        select flatten(categories) catl from dfs.`/tmp/yelp_academic_dataset_business.json` b\n" +
          "    )  celltbl");
    }

  }

  @Test
  public void testDrill1665() throws Exception {
    if(RUN_ADVANCED_TESTS){
      test("select id, flatten(evnts) as rpt from dfs.`/tmp/drill1665.json`");
    }

  }

  @Test
  public void testFlattenComplexRepeatedMap() throws Exception {
    test("select a, flatten(r_map_1), flatten(r_map_2) from cp.`/store/json/complex_repeated_map.json`");
  }

  @Test
  public void testFlatten2_levelRepeatedMap() throws Exception {
    test("select flatten(rm) from cp.`/store/json/2_level_repeated_map.json`");
  }

  @Test
  public void testDrill_1770() throws Exception {
    test("select flatten(sub.fk.`value`) from (select flatten(kvgen(map)) fk from cp.`/store/json/nested_repeated_map.json`) sub");
  }


  @Test //DRILL-2254
  public void testSingleFlattenFromNestedRepeatedList() throws Exception {
    final String query = "select t.uid, flatten(t.odd) odd from cp.`project/complex/a.json` t";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2254-result-single.json")
        .build()
        .run();
  }

  @Test //DRILL-2254 supplementary
  public void testMultiFlattenFromNestedRepeatedList() throws Exception {
    final String query = "select t.uid, flatten(flatten(t.odd)) odd from cp.`project/complex/a.json` t";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2254-result-multi.json")
        .build()
        .run();
  }

  @Test //DRILL-2254 supplementary
  public void testSingleMultiFlattenFromNestedRepeatedList() throws Exception {
    final String query = "select t.uid, flatten(t.odd) once, flatten(flatten(t.odd)) twice from cp.`project/complex/a.json` t";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2254-result-mix.json")
        .build()
        .run();
  }


  @Test
  public void testDrill_2013() throws Exception {
    testBuilder()
            .sqlQuery("select flatten(complex), rownum from cp.`/store/json/test_flatten_mappify2.json` where rownum > 5")
            .expectsEmptyResultSet()
            .build().run();
  }

  @Test
  public void testDRILL_2106() throws Exception {
    testBuilder()
        .sqlQuery("select rl, flatten(rl) frl from (select `integer`, flatten(rl) as rl from cp.`jsoninput/input2.json`)")
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2106-result.json")
        .go();

    testBuilder()
        .sqlQuery("select rl, flatten(rl) frl from (select flatten(rl) as rl, `integer` from cp.`jsoninput/input2.json`)")
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2106-result.json")
        .go();

  }

  @Test // see DRILL-2146
  public void testFalttenWithStar() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/sample.json").toURI().toString();
    String q1 = String.format("select *, flatten(j.topping) tt, flatten(j.batters.batter) bb, j.id " +
        "from dfs_test.`%s` j " +
        "where j.type = 'donut'", root);
    String q2 = String.format("select *, flatten(j.topping) tt, flatten(j.batters.batter) bb, j.id, j.type " +
        "from dfs_test.`%s` j " +
        "where j.type = 'donut'", root);

    test(q1);
    test(q2);
  }

  @Test // see DRILL-2012
  public void testMultipleFalttenWithWhereClause() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/sample.json").toURI().toString();
    String q1 = String.format("select flatten(j.topping) tt " +
        "from dfs_test.`%s` j " +
        "where j.type = 'donut'", root);
    String q2 = String.format("select j.type, flatten(j.topping) tt " +
         "from dfs_test.`%s` j " +
         "where j.type = 'donut'", root);

    test(q1);
    test(q2);
  }

  @Test //DRILL-2099
  public void testFlattenAfterSort() throws Exception {
    String query = "select flatten(s1.rms.rptd) rptds from " +
        "(select d.uid uid, flatten(d.map.rm) rms from cp.`jsoninput/flatten_post_sort.json` d order by d.uid) s1";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("flatten/drill-2099-result.json")
        .go();
  }

  @Test //DRILL-2268
  public void testFlattenAfterJoin1() throws Exception {
    String query = "select flatten(sub1.events) flat_events  from "+
      "(select t1.events events from cp.`complex/json/flatten_join.json` t1 "+
      "inner join cp.`complex/json/flatten_join.json` t2 on t1.id=t2.id) sub1";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("complex/drill-2268-1-result.json")
      .go();
  }

  @Test //DRILL-2268
  public void testFlattenAfterJoin2() throws Exception {
    String query = "select flatten(t1.events) flat_events from cp.`complex/json/flatten_join.json` t1 " +
      "inner join cp.`complex/json/flatten_join.json` t2 on t1.id=t2.id";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("complex/drill-2268-2-result.json")
      .go();
  }

  @Test //DRILL-2268
  public void testFlattenAfterJoin3() throws Exception {
    String query = "select flatten(sub1.lst_lst) flat_lst_lst from "+
      "(select t1.lst_lst lst_lst from cp.`complex/json/flatten_join.json` t1 "+
      "inner join cp.`complex/json/flatten_join.json` t2 on t1.id=t2.id) sub1";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("complex/drill-2268-3-result.json")
      .go();
  }


}
