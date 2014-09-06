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
package org.apache.drill.exec.physical.impl.writer;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class TestParquetWriter extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestParquetWriter.class);

  static FileSystem fs;

  private static final boolean VERBOSE_DEBUG = false;

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.name.default", "local");

    fs = FileSystem.get(conf);
  }

  @Test
  public void testSimple() throws Exception {
    String selection = "*";
    String inputTable = "cp.`employee.json`";
    runTestAndValidate(selection, selection, inputTable, "employee_parquet");
  }

  @Test
  public void testComplex() throws Exception {
    String selection = "*";
    String inputTable = "cp.`donuts.json`";
    runTestAndValidate(selection, selection, inputTable, "donuts_json");
  }

  @Test
  public void testComplexRepeated() throws Exception {
    String selection = "*";
    String inputTable = "cp.`testRepeatedWrite.json`";
    runTestAndValidate(selection, selection, inputTable, "repeated_json");
  }

  @Test
  public void testCastProjectBug_Drill_929() throws Exception {
    String selection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, cast(L_COMMITDATE as DATE) as COMMITDATE, cast(L_RECEIPTDATE as DATE) AS RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";
    String validationSelection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE,COMMITDATE ,RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";
    String inputTable = "cp.`tpch/lineitem.parquet`";
    String query = String.format("SELECT %s FROM %s", selection, inputTable);
    List<QueryResultBatch> expected = testSqlWithResults(query);
    BatchSchema schema = null;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    List<Map> expectedRecords = new ArrayList<>();
    // read the data out of the results, the error manifested itself upon call of getObject on the vectors as they had contained deadbufs
    addToMaterializedResults(expectedRecords, expected, loader, schema);
    for (QueryResultBatch result : expected) {
      result.release();
    }
}

  @Test
  public void testTPCHReadWrite1() throws Exception {
    String inputTable = "cp.`tpch/lineitem.parquet`";
    runTestAndValidate("*", "*", inputTable, "lineitem_parquet_all");
  }

  @Test
  public void testTPCHReadWrite1_date_convertedType() throws Exception {
    String selection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, cast(L_COMMITDATE as DATE) as L_COMMITDATE, cast(L_RECEIPTDATE as DATE) AS L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";
    String validationSelection = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, " +
        "L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE,L_COMMITDATE ,L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT";
    String inputTable = "cp.`tpch/lineitem.parquet`";
    runTestAndValidate(selection, validationSelection, inputTable, "lineitem_parquet_converted");
  }

  @Test
  public void testTPCHReadWrite2() throws Exception {
    String inputTable = "cp.`tpch/customer.parquet`";
    runTestAndValidate("*", "*", inputTable, "customer_parquet");
  }

  @Test
  public void testTPCHReadWrite3() throws Exception {
    String inputTable = "cp.`tpch/nation.parquet`";
    runTestAndValidate("*", "*", inputTable, "nation_parquet");
  }

  @Test
  public void testTPCHReadWrite4() throws Exception {
    String inputTable = "cp.`tpch/orders.parquet`";
    runTestAndValidate("*", "*", inputTable, "orders_parquet");
  }

  @Test
  public void testTPCHReadWrite5() throws Exception {
    String inputTable = "cp.`tpch/part.parquet`";
    runTestAndValidate("*", "*", inputTable, "part_parquet");
  }

  @Test
  public void testTPCHReadWrite6() throws Exception {
    String inputTable = "cp.`tpch/partsupp.parquet`";
    runTestAndValidate("*", "*", inputTable, "partsupp_parquet");
  }

  @Test
  public void testTPCHReadWrite7() throws Exception {
    String inputTable = "cp.`tpch/region.parquet`";
    runTestAndValidate("*", "*", inputTable, "region_parquet");
  }

  @Test
  public void testTPCHReadWrite8() throws Exception {
    String inputTable = "cp.`tpch/supplier.parquet`";
    runTestAndValidate("*", "*", inputTable, "supplier_parquet");
  }

  // working to create an exhaustive test of the format for this one. including all convertedTypes
  // will not be supporting interval for Beta as of current schedule
  // Types left out:
  // "TIMESTAMPTZ_col"
  @Test
  public void testRepeated() throws Exception {
    String inputTable = "cp.`parquet/basic_repeated.json`";
    runTestAndValidate("*", "*", inputTable, "basic_repeated");
  }

  // TODO - this is failing due to the parquet behavior of allowing repeated values to reach across
  // pages. This broke our reading model a bit, but it is possible to work around.
  @Test
  public void testRepeatedDouble() throws Exception {
    String inputTable = "cp.`parquet/repeated_double_data.json`";
    runTestAndValidate("*", "*", inputTable, "repeated_double_parquet");
  }

  @Test
  public void testRepeatedLong() throws Exception {
    String inputTable = "cp.`parquet/repeated_integer_data.json`";
    runTestAndValidate("*", "*", inputTable, "repeated_int_parquet");
  }

  @Test
  @Ignore
  public void testRepeatedBool() throws Exception {
    String inputTable = "cp.`parquet/repeated_bool_data.json`";
    runTestAndValidate("*", "*", inputTable, "repeated_bool_parquet");
  }

  @Test
  public void testNullReadWrite() throws Exception {
    String inputTable = "cp.`parquet/null_test_data.json`";
    runTestAndValidate("*", "*", inputTable, "nullable_test");
  }

  @Test
  public void testDecimal() throws Exception {
    String selection = "cast(salary as decimal(8,2)) as decimal8, cast(salary as decimal(15,2)) as decimal15, " +
        "cast(salary as decimal(24,2)) as decimal24, cast(salary as decimal(38,2)) as decimal38";
    String validateSelection = "decimal8, decimal15, decimal24, decimal38";
    String inputTable = "cp.`employee.json`";
    runTestAndValidate(selection, validateSelection, inputTable, "parquet_decimal");
  }

  @Test
  public void testMulipleRowGroups() throws Exception {
    try {
      //test(String.format("ALTER SESSION SET `%s` = %d", ExecConstants.PARQUET_BLOCK_SIZE, 1*1024*1024));
      String selection = "mi";
      String inputTable = "cp.`customer.json`";
      int count = testRunAndPrint(UserBitShared.QueryType.SQL, "select mi from cp.`customer.json`");
      System.out.println(count);
      runTestAndValidate(selection, selection, inputTable, "foodmart_customer_parquet");
    } finally {
      test(String.format("ALTER SESSION SET `%s` = %d", ExecConstants.PARQUET_BLOCK_SIZE, 512*1024*1024));
    }
  }


  @Test
  public void testDate() throws Exception {
    String selection = "cast(hire_date as DATE) as hire_date";
    String validateSelection = "hire_date";
    String inputTable = "cp.`employee.json`";
    runTestAndValidate(selection, validateSelection, inputTable, "foodmart_employee_parquet");
  }

  public void compareParquetReaders(String selection, String table) throws Exception {
    test("alter system set `store.parquet.use_new_reader` = true");
    List<QueryResultBatch> expected = testSqlWithResults("select " + selection + " from " + table);
    test("alter system set `store.parquet.use_new_reader` = false");
    List<QueryResultBatch> results = testSqlWithResults("select " + selection + " from " + table);
    compareResults(expected, results);
    for (QueryResultBatch result : results) {
      result.release();
    }
    for (QueryResultBatch result : expected) {
      result.release();
    }
  }

  @Test
  public void testReadVoter() throws Exception {
    compareParquetReaders("*", "dfs.`/tmp/voter.parquet`");
  }

  @Test
  public void testReadSf_100_supplier() throws Exception {
    compareParquetReaders("*", "dfs.`/tmp/sf100_supplier.parquet`");
  }

  @Test
  public void testParquetRead_checkNulls_NullsFirst() throws Exception {
    compareParquetReaders("*", "dfs.`/tmp/parquet_with_nulls_should_sum_100000_nulls_first.parquet`");
  }

  // TODO - fix this, currently hanging
  @Ignore
  @Test
  public void testParquetRead_checkNulls() throws Exception {
    compareParquetReaders("*", "dfs.`/tmp/parquet_with_nulls_should_sum_100000.parquet`");
  }

  @Test
  public void test958_sql() throws Exception {
    compareParquetReaders("ss_ext_sales_price",  "dfs.`/tmp/store_sales`");
  }

  @Test
  public void testDrill_1314() throws Exception {
    compareParquetReaders("l_partkey ", "dfs.`/tmp/drill_1314.parquet`");
  }

  @Ignore // too big for memory, need to figure out a better way to compare result sets
  @Test
  public void testDrill_1314_all_columns() throws Exception {
    compareParquetReaders("*", "dfs.`/tmp/drill_1314.parquet`");
  }

  @Test
  public void testParquetRead_checkShortNullLists() throws Exception {
    compareParquetReaders("*", "dfs.`/tmp/short_null_lists.parquet`");
  }

  @Test
  public void testParquetRead_checkStartWithNull() throws Exception {
    compareParquetReaders("*", "dfs.`/tmp/start_with_null.parquet`");
  }

  @Test
  public void testParquetReadWebReturns() throws Exception {
    compareParquetReaders("wr_returning_customer_sk", "dfs.`/tmp/web_returns`");
  }

  public void runTestAndValidate(String selection, String validationSelection, String inputTable, String outputFile) throws Exception {

    Path path = new Path("/tmp/" + outputFile);
    if (fs.exists(path)) {
      fs.delete(path, true);
    }

    test("use dfs.tmp");
//    test("ALTER SESSION SET `planner.add_producer_consumer` = false");
    String query = String.format("SELECT %s FROM %s", selection, inputTable);
    String create = "CREATE TABLE " + outputFile + " AS " + query;
    String validateQuery = String.format("SELECT %s FROM " + outputFile, validationSelection);
    test(create);
    List<QueryResultBatch> expected = testSqlWithResults(query);
    List<QueryResultBatch> results = testSqlWithResults(validateQuery);
    compareResults(expected, results);
    for (QueryResultBatch result : results) {
      result.release();
    }
    for (QueryResultBatch result : expected) {
      result.release();
    }
  }

  public void addToMaterializedResults(List<Map> materializedRecords,  List<QueryResultBatch> records, RecordBatchLoader loader,
                                       BatchSchema schema) throws SchemaChangeException, UnsupportedEncodingException {
    long totalRecords = 0;
    QueryResultBatch batch;
    for (int i = 0; i < records.size(); i++) {
      batch = records.get(0);
      loader.load(batch.getHeader().getDef(), batch.getData());
      if (schema == null) {
        schema = loader.getSchema();
      }
      logger.debug("reading batch with " + loader.getRecordCount() + " rows, total read so far " + totalRecords);
      totalRecords += loader.getRecordCount();
      for (int j = 0; j < loader.getRecordCount(); j++) {
        HashMap<String, Object> record = new HashMap<>();
        for (VectorWrapper w : loader) {
          Object obj = w.getValueVector().getAccessor().getObject(j);
          if (obj != null) {
            if (obj instanceof Text) {
              obj = obj.toString();
              if (obj.equals("")) {
                System.out.println(w.getField());
              }
            }
            else if (obj instanceof byte[]) {
              obj = new String((byte[]) obj, "UTF-8");
            }
            record.put(w.getField().toExpr(), obj);
          }
          record.put(w.getField().toExpr(), obj);
        }
        materializedRecords.add(record);
      }
      records.remove(0);
      batch.release();
      loader.clear();
    }
  }

  public void compareResults(List<QueryResultBatch> expected, List<QueryResultBatch> result) throws Exception {
    List<Map> expectedRecords = new ArrayList<>();
    List<Map> actualRecords = new ArrayList<>();

    BatchSchema schema = null;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    addToMaterializedResults(expectedRecords, expected, loader, schema);
    addToMaterializedResults(actualRecords, result, loader, schema);
//    Assert.assertEquals("Different number of records returned", expectedRecords.size(), actualRecords.size());

    StringBuilder missing = new StringBuilder();
    int i = 0;
    int counter = 0;
    int missmatch;
    for (Map<String, Object> record : expectedRecords) {
      missmatch = 0;
      for (String column : record.keySet()) {
        if (  actualRecords.get(i).get(column) == null && record.get(column) == null ) {
          if (VERBOSE_DEBUG) System.out.println("(1) at position " + counter + " column '" + column + "' matched value:  " + record.get(column)  );
          continue;
        }
        if (actualRecords.get(i).get(column) == null) {
          if (VERBOSE_DEBUG) System.out.println("unexpected null at position " + counter + " column '" + column + "' should have been:  " + record.get(column)  );
          continue;
        }
        if ( (actualRecords.get(i).get(column) == null && record.get(column) == null) || ! actualRecords.get(i).get(column).equals(record.get(column))) {
          missmatch++;
          System.out.println("at position " + counter + " column '" + column + "' mismatched values, expected: " + record.get(column) + " but received " + actualRecords.get(i).get(column));
        } else {
          if (VERBOSE_DEBUG) System.out.println("at position " + counter + " column '" + column + "' matched value:  " + record.get(column)  );
        }
      }
      if ( ! actualRecords.get(i).equals(record)) {
        System.out.println("mismatch at position " + counter );
        missing.append(missmatch);
        missing.append(",");
      } else {
      }
      counter++;
      if (counter % 100000 == 0 ) {
        System.out.println("checked so far:" + counter);
      }
      i++;
    }
    logger.debug(missing.toString());
    System.out.println(missing);
  }
}
