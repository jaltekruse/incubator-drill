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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestTestFramework extends BaseTestQuery {

  private static String CSV_COLS = " cast(columns[0] as bigint) employee_id, columns[1] as first_name, columns[2] as last_name ";

  // TODO - add enforcement of column ordering

  @Test
  public void testCSVVerification() throws Exception {
    testBuilder()
        .sqlQuery("select employee_id, first_name, last_name from cp.`testframework/small_test_data.json`")
        .ordered()
        .csvBaselineFile("testframework/small_test_data.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("employee_id", "first_name", "last_name")
        .build().run();
  }

  @Test
  public void testBaselineValsVerification() throws Exception {
    testBuilder()
        .sqlQuery("select employee_id, first_name, last_name from cp.`testframework/small_test_data.json` limit 1")
        .baselineColumns("employee_id", "first_name", "last_name")
        // TODO - make sure this have more than one row allowed
        .baselineValues(12l, "Jewel", "Creek")
        .build().run();
  }

  @Test
  public void testCSVVerification_missing_records_fails() throws Exception {
    try {
    testBuilder()
        .sqlQuery("select employee_id, first_name, last_name from cp.`testframework/small_test_data.json`")
        .ordered()
        .csvBaselineFile("testframework/small_test_data_extra.tsv")
        .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("employee_id", "first_name", "last_name")
        .build().run();
    } catch (AssertionError ex) {
      assertEquals("Incorrect number of rows returned by query. expected:<7> but was:<5>", ex.getMessage());
      // this indicates successful completion of the test
      return;
    }
    throw new Exception("Test framework verification failed, expected failure on missing records.");
  }

  @Test
  public void testCSVVerification_extra_records_fails() throws Exception {
    try {
      testBuilder()
          .sqlQuery("select " + CSV_COLS + " from cp.`testframework/small_test_data_extra.tsv`")
          .ordered()
          .csvBaselineFile("testframework/small_test_data.tsv")
          .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR)
          .baselineColumns("employee_id", "first_name", "last_name")
          .build().run();
    } catch (AssertionError ex) {
      assertEquals("Incorrect number of rows returned by query. expected:<5> but was:<7>", ex.getMessage());
      // this indicates successful completion of the test
      return;
    }
    throw new Exception("Test framework verification failed, expected failure for extra records.");
  }

  @Test
  public void testCSVVerification_extra_column_fails() throws Exception {
    try {
      testBuilder()
          .sqlQuery("select " + CSV_COLS + ", columns[3] as address from cp.`testframework/small_test_data_extra_col.tsv`")
          .ordered()
          .csvBaselineFile("testframework/small_test_data.tsv")
          .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR)
          .baselineColumns("employee_id", "first_name", "last_name")
          .build().run();
    } catch (AssertionError ex) {
      assertEquals("Unexpected extra column `address` returned by query.", ex.getMessage());
      // this indicates successful completion of the test
      return;
    }
    throw new Exception("Test framework verification failed, expected failure on extra column.");
  }

  @Test
  public void testCSVVerification_missing_column_fails() throws Exception {
    try {
      testBuilder()
          .sqlQuery("select employee_id, first_name, last_name from cp.`testframework/small_test_data.json`")
          .ordered()
          .csvBaselineFile("testframework/small_test_data_extra_col.tsv")
          .baselineTypes(TypeProtos.MinorType.BIGINT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR)
          .baselineColumns("employee_id", "first_name", "last_name", "address")
          .build().run();
    } catch (Exception ex) {
      assertEquals("Expected column(s) `address`,  not found in result set.", ex.getMessage());
      // this indicates successful completion of the test
      return;
    }
    throw new Exception("Test framework verification failed, expected failure on missing column.");
  }

  @Test
  public void testCSVVerificationOfTypes() throws Throwable {
    try {
    testBuilder()
        .sqlQuery("select employee_id, first_name, last_name from cp.`testframework/small_test_data.json`")
        .ordered()
        .csvBaselineFile("testframework/small_test_data.tsv")
        .baselineTypes(TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("employee_id", "first_name", "last_name")
        .build().run();
    } catch (Exception ex) {
      assertEquals("at position 0 column '`employee_id`' mismatched values, expected: 12(Integer) but received 12(Long)", ex.getMessage());
      // this indicates successful completion of the test
      return;
    }
    throw new Exception("Test framework verification failed, expected failure on type check.");
  }

  @Test
  public void testCSVVerificationOfOrder_checkFailure() throws Throwable {
    try {
      testBuilder()
          .sqlQuery("select columns[0] as employee_id, columns[1] as first_name, columns[2] as last_name from cp.`testframework/small_test_data_reordered.tsv`")
          .ordered()
          .csvBaselineFile("testframework/small_test_data.tsv")
          .baselineColumns("employee_id", "first_name", "last_name")
          .build().run();
    } catch (Exception ex) {
      assertEquals("at position 0 column '`first_name`' mismatched values, expected: Jewel(String) but received Peggy(String)", ex.getMessage());
      // this indicates successful completion of the test
      return;
    }
    throw new Exception("Test framework verification failed, expected failure on order check.");
  }

  @Test
  public void testCSVVerificationOfUnorderedComparison() throws Throwable {
    testBuilder()
        .sqlQuery("select columns[0] as employee_id, columns[1] as first_name, columns[2] as last_name from cp.`testframework/small_test_data_reordered.tsv`")
        .unOrdered()
        .csvBaselineFile("testframework/small_test_data.tsv")
        .baselineColumns("employee_id", "first_name", "last_name")
        .build().run();
  }

  // TODO - enable more advanced type handling for JSON, currently basic support works
  // add support for type information taken from test query, or explicit type expectations
  @Test
  public void testBasicJSON() throws Exception {
    testBuilder()
        .sqlQuery("select * from cp.`scan_json_test_3.json`")
        .ordered()
        .jsonBaselineFile("/scan_json_test_3.json")
        .build().run();

    testBuilder()
        .sqlQuery("select * from cp.`scan_json_test_3.json`")
        .unOrdered() // Check other verification method with same files
        .jsonBaselineFile("/scan_json_test_3.json")
        .build().run();
  }

  @Test
  public void testComplexJSON_all_text() throws Exception {
    testBuilder()
        .sqlQuery("select * from cp.`store/json/schema_change_int_to_string.json`")
        .optionSettingQueriesForTestQuery("alter system set `store.json.all_text_mode` = true")
        .ordered()
        .jsonBaselineFile("store/json/schema_change_int_to_string.json")
        .optionSettingQueriesForBaseline("alter system set `store.json.all_text_mode` = true")
        .build().run();

    testBuilder()
        .sqlQuery("select * from cp.`store/json/schema_change_int_to_string.json`")
        .optionSettingQueriesForTestQuery("alter system set `store.json.all_text_mode` = true")
        .unOrdered() // Check other verification method with same files
        .jsonBaselineFile("store/json/schema_change_int_to_string.json")
        .optionSettingQueriesForBaseline("alter system set `store.json.all_text_mode` = true")
        .build().run();
    test("alter system set `store.json.all_text_mode` = false");
  }

  @Test
  public void testRepeatedColumnMatching() throws Exception {
    try {
      testBuilder()
          .sqlQuery("select * from cp.`store/json/schema_change_int_to_string.json`")
          .optionSettingQueriesForTestQuery("alter system set `store.json.all_text_mode` = true")
          .ordered()
          .jsonBaselineFile("testframework/schema_change_int_to_string_non-matching.json")
          .optionSettingQueriesForBaseline("alter system set `store.json.all_text_mode` = true")
          .build().run();
    } catch (Exception ex) {
      assertEquals("at position 1 column '`field_1`' mismatched values, " +
          "expected: [\"5\",\"2\",\"3\",\"4\",\"1\",\"2\"](JsonStringArrayList) but received [\"5\"](JsonStringArrayList)",
          ex.getMessage());
      // this indicates successful completion of the test
      return;
    }
    throw new Exception("Test framework verification failed, expected failure on order check.");
  }

  // TODO - write a comment to discourage use of this
  // TODO - verifying errors
  // optiq has fusion model for plan and result check
  // text comparison for plan regression check
  // for ordered(), check the query for an order by
  @Test
  public void testCSVVerificationTypeMap() throws Throwable {
    Map<SchemaPath, TypeProtos.MinorType> typeMap = new HashMap<>();
    typeMap.put(TestBuilder.parsePath("first_name"), TypeProtos.MinorType.VARCHAR);
    typeMap.put(TestBuilder.parsePath("employee_id"), TypeProtos.MinorType.INT);
    typeMap.put(TestBuilder.parsePath("last_name"), TypeProtos.MinorType.VARCHAR);
    testBuilder()
        .sqlQuery("select cast(columns[0] as int) employee_id, columns[1] as first_name, columns[2] as last_name from cp.`testframework/small_test_data_reordered.tsv`")
        .unOrdered()
        .csvBaselineFile("testframework/small_test_data.tsv")
        .baselineColumns("employee_id", "first_name", "last_name")
        // This should work without this line because of the default type casts added based on the types that come out of the test query.
        // To write a test that enforces strict typing you must pass type information using a CSV with a list of types,
        // or any format with a Map of types like is constructed above and include the call to pass it into the test, which is commented out below
        //.baselineTypes(typeMap)
        .build().run();

    typeMap.clear();
    typeMap.put(TestBuilder.parsePath("first_name"), TypeProtos.MinorType.VARCHAR);
    // This is the wrong type intentionally to ensure failures happen when expected
    typeMap.put(TestBuilder.parsePath("employee_id"), TypeProtos.MinorType.VARCHAR);
    typeMap.put(TestBuilder.parsePath("last_name"), TypeProtos.MinorType.VARCHAR);

    try {
    testBuilder()
        .sqlQuery("select cast(columns[0] as int) employee_id, columns[1] as first_name, columns[2] as last_name from cp.`testframework/small_test_data_reordered.tsv`")
        .unOrdered()
        .csvBaselineFile("testframework/small_test_data.tsv")
        .baselineColumns("employee_id", "first_name", "last_name")
        .baselineTypes(typeMap)
        .build().run();
    } catch (Exception ex) {
      // this indicates successful completion of the test
      return;
    }
    throw new Exception("Test framework verification failed, expected failure on type check.");
  }

}
