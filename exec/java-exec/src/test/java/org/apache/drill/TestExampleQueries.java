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

import static org.junit.Assert.assertTrue;

import io.netty.buffer.Unpooled;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.QueryResultAccessor;
import org.junit.Test;

public class TestExampleQueries extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExampleQueries.class);

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
  public void testSelectWithLimit() throws Exception{
    test("select employee_id,  first_name, last_name from cp.`employee.json` order by employee_id limit 5 offset 10");
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
  public void testShowTables() throws Exception{
    testSqlWithValidator("SHOW TABLES", new Validator() {
      @Override
      public void validate(QueryResultAccessor accessor) throws Exception {
        String[][] expected = {
            {"INFORMATION_SCHEMA", "VIEWS"},
            {"INFORMATION_SCHEMA", "COLUMNS"},
            {"INFORMATION_SCHEMA", "TABLES"},
            {"INFORMATION_SCHEMA", "CATALOGS"},
            {"INFORMATION_SCHEMA", "SCHEMATA"}
        };

        VarCharHolder schema = new VarCharHolder();
        schema.buffer = Unpooled.wrappedBuffer(new byte[100]);
        schema.start = 0;
        schema.end = 100;
        VarCharHolder table = new VarCharHolder();
        table.buffer = Unpooled.wrappedBuffer(new byte[100]);
        table.start = 0;
        table.end = 100;

        while (accessor.nextBatch()) {
          int rows = accessor.getRowCount();
          for (int i=0; i<rows; i++) {
            accessor.getFieldById(0, i, schema);
            accessor.getFieldById(1, i, table);

            assertTrue(expected[i][0].equals(schema.toString()));
            assertTrue(expected[i][1].equals(table.toString()));
          }
        }
      }
    });
  }
}
