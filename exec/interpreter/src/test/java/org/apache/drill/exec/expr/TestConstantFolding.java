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
package org.apache.drill.exec.expr;

import org.apache.drill.PlanTestBase;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.PrintWriter;

public class TestConstantFolding extends PlanTestBase {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  // This should run as a @BeforeClass, but these methods must be defined static.
  // Unfortunately, the temporary folder with an @Rule annotation cannot be static, this issue
  // has been fixed in a newer version of JUnit
  // http://stackoverflow.com/questions/2722358/junit-rule-temporaryfolder
  public void createFiles() throws Exception{
    File bigFolder = folder.newFolder("bigfile");
    File bigFile = new File (bigFolder, "bigfile.csv");
    PrintWriter out = new PrintWriter(bigFile);
    out.println("1,2,3");
    out.println("1,2,3");
    out.println("1,2,3");
    out.println("1,2,3");
    out.println("1,2,3");
    out.println("1,2,3");
    out.close();

    File smallFolder = folder.newFolder("smallfile");
    File smallFile = new File (smallFolder, "smallfile.csv");
    out = new PrintWriter(smallFile);
    out.println("1,2,3");
    out.close();
  }

  @Test
  public void testConstantFolding_allTypes() throws Exception {

    test("alter system set `store.json.all_text_mode` = true;");

    String query2 = "SELECT " +
        "int_col, bigint_col " +
//        "       Cast( `int_col` AS             INT)             int_col,  " +
//        "       Cast( `bigint_col` AS          BIGINT)          bigint_col,  " +
//        "       Cast( `decimal9_col` AS        DECIMAL)         decimal9_col,  " +
//        "       Cast( `decimal18_col` AS       DECIMAL(18,9))   decimal18_col,  " +
//        "       Cast( `decimal28sparse_col` AS DECIMAL(28, 14)) decimal28sparse_col,  " +
//        "       Cast( `decimal38sparse_col` AS DECIMAL(38, 19)) decimal38sparse_col,  " +
//        "       Cast( `date_col` AS            DATE)            date_col,  " +
//        "       Cast( `time_col` AS            TIME)            time_col,  " +
//        "       Cast( `timestamp_col` AS TIMESTAMP)             timestamp_col,  " +
//        "       Cast( `float4_col` AS FLOAT)                    float4_col,  " +
//        "       Cast( `float8_col` AS DOUBLE)                   float8_col,  " +
//        "       Cast( `bit_col` AS       BOOLEAN)                     bit_col,  " +
//        "       Cast( `varchar_col` AS   VARCHAR(65000))              varchar_col,  " +
//        "       `varbinary_col`            varbinary_col,  " +
//        "       cast( `intervalyear_col` as INTERVAL YEAR)            intervalyear_col,  " +
//        "       cast( `intervalday_col` as INTERVAL DAY )              intervalday_col  " +
        "FROM   cp.`/parquet/alltypes.json`  " +
        "WHERE  `int_col` = 1 + 0 " +
        "AND    `bigint_col` = 1 + 0  "
        // TODO - execution is broken here, the function is using a utility to convert from varchar to int, not able to
        // handle decimals
//        "AND    `decimal9_col` = cast( '1.0' AS                        decimal)  " +
//        "AND    `decimal18_col` = cast( '123456789.000000000' AS       decimal(18,9))  " +
//        "AND    `decimal28sparse_col` = cast( '123456789.000000000' AS decimal(28, 14))  " +
//        "AND    `decimal38sparse_col` = cast( '123456789.000000000' AS decimal(38, 19))  " +
//        "AND    `date_col` = cast( '1995-01-01' AS                     date)  " +
//        "AND    `time_col` = cast( '01:00:00' AS                       time)  " +
//        "AND    `timestamp_col` = cast( '1995-01-01 01:00:10.000' AS timestamp)  " +
//        "AND    `float4_col` = cast( '1' AS float)  " +
//        "AND    `float8_col` = cast( '1' AS DOUBLE)  " +
//        "AND    `bit_col` = cast( 'false' AS        boolean)  " +
//        "AND    `varchar_col` = cast( 'qwerty' AS   varchar(65000))  " +
//        "AND    `varbinary_col` = converttonullablevarbinary('qwerty')  " +
//        "AND    `intervalyear_col` = converttonullableintervalyear( 'P1Y')  " +
//        "AND    `intervalday_col` = converttonullableintervalday( 'P1D' )"
        ;


    test(query2);
  }

  @Test
  public void testConstExprFolding_withPartitionPrune() throws Exception {
    createFiles();
    String path = folder.getRoot().toPath().toString();
    testPlanOneExpectedPatternOneExcluded(
        "select * from dfs.`" + path + "/*/*.csv` where dir0 = concat('small','file')",
        "smallfile",
        "bigfile");
  }

  @Test
  public void testConstExprFolding_maxDir0() throws Exception {
    createFiles();
    String path = folder.getRoot().toPath().toString();
    testPlanOneExpectedPatternOneExcluded(
        "select * from dfs.`" + path + "/*/*.csv` where dir0 = maxdir('dfs','root','" + path + "')",
        "smallfile",
        "bigfile");
  }

  @Test
  public void testConstExprFolding_nonDirFilter() throws Exception {
    testPlanOneExpectedPatternOneExcluded(
        "select * from cp.`test_input.csv` where columns[0] = 2+2",
        "Filter\\(condition=\\[=\\(ITEM\\(\\$[0-9]+, 0\\), 4\\)",
        "Filter\\(condition=\\[=\\(ITEM\\(\\$[0-9]+, 0\\), \\+\\(2, 2\\)\\)");
  }

  @Test
  public void testConstExprFolding_dontFoldRandom() throws Exception {
    testPlanOneExpectedPatternOneExcluded(
        "select * from cp.`test_input.csv` where columns[0] = random()",
        "Filter\\(condition=\\[=\\(ITEM\\(\\$[0-9]+, 0\\), RANDOM\\(\\)",
        "Filter\\(condition=\\[=\\(ITEM\\(\\$[0-9]+, 0\\), [0-9\\.]+");
  }

  @Test
  public void testConstExprFolding_ToLimit0() throws Exception {
    testPlanOneExpectedPatternOneExcluded(
        "select * from cp.`test_input.csv` where 1=0",
        "Limit\\(offset=\\[0\\], fetch=\\[0\\]\\)",
        "Filter\\(condition=\\[=\\(1, 0\\)\\]\\)");
  }

  // Despite a comment indicating that the plan generated by the ReduceExpressionRule
  // should be set to be always preferable to the input rel, I cannot get it to
  // produce a plan with the reduced result. I can trace through where the rule is fired
  // and I can see that the expression is being evaluated and the constant is being
  // added to a project, but this is not part of the final plan selected. May
  // need to open a calcite bug.
  // Tried to disable the calc and filter rules, only leave the project one, didn't help.
  @Ignore("DRILL-2218")
  @Test
  public void testConstExprFolding_InSelect() throws Exception {
    testPlanOneExcludedPattern("select columns[0], 3+5 from cp.`test_input.csv`",
                               "EXPR\\$[0-9]+=\\[\\+\\(3, 5\\)\\]");
  }
}
