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

import static org.apache.drill.common.types.TypeProtos.MinorType.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.DrillTestWrapper;
import org.apache.drill.common.types.DataMode;
import org.apache.drill.common.types.MinorType;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.HyperVectorValueIterator;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.ValueVector;
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

  // This is a list of all of the defined physical types according to the protobuf definition that
  // have not been fully implemented or are not currently supported
  private List<TypeProtos.MinorType> toSkip = Lists.newArrayList(
      // LATE - cannot appear in execution
      // MAP - cannot be casted/mocked
      // List - cannot be casted/mocked
      // Dense decimal types  - in the process of being deprecated
      LATE, DECIMAL28DENSE, DECIMAL38DENSE,
      MAP, LIST, MONEY, TIMETZ, TIMESTAMPTZ,
      // FixedChar, Fixed16cahr, FixedBinary - not yet implimented
      // NULL - not used currently
      // GENERIC OBJECT - not used currently, see MAP
      FIXEDCHAR, FIXED16CHAR, FIXEDBINARY, NULL, GENERIC_OBJECT,
      // INTERVAL - not recognized as valid in parsing, this fails, cast( col_1 as inteval)
      INTERVAL
      // TODO - cast is resolving to a boolean for TINYINT
      , TINYINT, SMALLINT
      // TODO - DRILL-1687: these are not documented on the WIKI, they are not currently supported but have many incorrect generated
      // functions written against them
      , UINT1, UINT2, UINT4, UINT8
      // TODO - fix mock data generation to be valid dates
//      , TypeProtos.MinorType.DATE, TypeProtos.MinorType.TIMESTAMP, TypeProtos.MinorType.TIME
      // TODO - re-enable this, fix parquet writer for decimal, or the DecimalUtiltiy which is providing byte[] for the wrap data
//      ,TypeProtos.MinorType.DECIMAL28SPARSE, TypeProtos.MinorType.DECIMAL38SPARSE
  );

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
  public void testAllDataTypes() throws Exception {
    String query = "SELECT ";
    List<String> columnsAndCastsAndComparisons = new ArrayList();
    List<String> columnsAndCasts = new ArrayList();
    List<String> columns = new ArrayList();

    query = "select INT_col,BIGINT_col,DECIMAL9_col,DECIMAL18_col,DECIMAL28SPARSE_col,DECIMAL38SPARSE_col,DATE_col,TIME_col,TIMESTAMP_col,FLOAT4_col,FLOAT8_col,BIT_col,VARCHAR_col,VAR16CHAR_col,VARBINARY_col,INTERVALYEAR_col,INTERVALDAY_col from cp.`/parquet/alltypes.json`";

    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    BatchSchema schema = null;
    String inputFile = "cp.`/parquet/alltypes.json`";

    List<QueryResultBatch> results = new ArrayList();
    List<Map> actualRecords = new ArrayList<>();
    System.out.println(query);
    results = BaseTestQuery.testRunAndReturn(UserBitShared.QueryType.SQL, query);
    DrillTestWrapper.addToMaterializedResults(actualRecords, results, loader, schema);

    int i = 0;
    for (TypeProtos.MinorType minorType : values()) {
      if (toSkip.contains(minorType)) {
        continue;
      }

      // TODO - remove - old strategy - select cast( INT_col as INT) INT_col, ... from inputTable t1 where t1.INT_col = cast('1' as INT) AND ..
      // select * FROM inputTable where cast(INT_col as INT) = castINT('1') AND ...
      try {
        String literal =  generateCast("`" + minorType.name().toUpperCase() + "_col`", minorType) + " = " + "cast" + minorType.name().toUpperCase() + "('" + actualRecords.get(0).get("`" + minorType.name().toUpperCase() + "_col`") + "')";
        String castExpr = generateCast("`" + minorType.name().toUpperCase() + "_col`", minorType) + " " + minorType.name().toUpperCase() + "_col";

        columnsAndCastsAndComparisons.add(literal);
        columns.add(castExpr);
        i++;

      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }

    // convertFrom takes varbinary, not varchar
    String query2 = "SELECT Cast( `int_col` AS             INT)             int_col, \n" +
        "       Cast( `bigint_col` AS          BIGINT)          bigint_col, \n" +
        "       Cast( `decimal9_col` AS        DECIMAL)         decimal9_col, \n" +
        "       Cast( `decimal18_col` AS       DECIMAL(18,9))   decimal18_col, \n" +
        "       Cast( `decimal28sparse_col` AS DECIMAL(28, 14)) decimal28sparse_col, \n" +
        "       Cast( `decimal38sparse_col` AS DECIMAL(38, 19)) decimal38sparse_col, \n" +
        "       Cast( `date_col` AS            DATE)            date_col, \n" +
        "       Cast( `time_col` AS            TIME)            time_col, \n" +
        "       Cast( `timestamp_col` AS TIMESTAMP)             timestamp_col, \n" +
        "       Cast( `float4_col` AS FLOAT)                    float4_col, \n" +
        "       Cast( `float8_col` AS DOUBLE)                   float8_col, \n" +
        "       Cast( `bit_col` AS       BOOLEAN)                     bit_col, \n" +
        "       Cast( `varchar_col` AS   VARCHAR(65000))              varchar_col, \n" +
        "       Cast( `varbinary_col` AS VARBINARY(65000))            varbinary_col, \n" +
        "       Cast( `intervalyear_col` AS INTERVAL year)            intervalyear_col, \n" +
        "       Cast( `intervalday_col` AS INTERVAL day)              intervalday_col \n" +
        "FROM   cp.`/parquet/alltypes.json` \n" +
        "WHERE  `int_col` = convert_fromint('1') \n" +
        "AND    `bigint_col` = convert_frombigint('1') \n" +

        // TODO - these are broken in execution, they are using utility to convert varchar to int, which considers .
        // a format exception
        "AND    `decimal9_col` = cast( '1.0' AS                        decimal)  " +
        "AND    `decimal18_col` = cast( '123456789.000000000' AS       decimal(18,9))  " +
        "AND    `decimal28sparse_col` = cast( '123456789.000000000' AS decimal(28, 14))  " +
        "AND    `decimal38sparse_col` = cast( '123456789.000000000' AS decimal(38, 19))  " +

        // TODO - these are not defined, not sure if this is intentional
//        "AND    `decimal9_col` = convert_fromdecimal9('1.0') \n" +
//        "AND    `decimal18_col` = convert_fromdecimal18('123456789.000000000') \n" +
//        "AND    `decimal28sparse_col` = convert_fromdecimal28sparse('123456789.000000000') \n" +
//        "AND    `decimal38sparse_col` = convert_fromdecimal38sparse('123456789.000000000') \n" +

        "AND    `date_col` = convert_fromDATE_EPOCH('1995-01-01') \n" +
        "AND    `time_col` = convert_fromTIME_EPOCH('01:00:00') \n" +
//        "AND    `timestamp_col` = convert_fromtimestamp('1995-01-01 01:00:10.000') \n" +
        "AND    `float4_col` = convert_fromFLOAT('1') \n" +
        "AND    `float8_col` = convert_fromDOUBLE('1') \n" +
        "AND    `bit_col` = convert_fromBOOLEAN_BYTE('0') \n" +
        // TODO - how to test folding here
        "AND    `varchar_col` = 'qwerty' \n" +
        "AND    `varbinary_col` = converttonullablevarbinary('qwerty') \n" +
        "AND    `intervalyear_col` = converttonullableintervalyear( 'P1Y') \n" +
        "AND    `intervalday_col` = converttonullableintervalday( 'P1D' )";

    // casts did not work, they were not being folding, trying convertfrom functions instead

    // select cast( INT_col as INT) INT_col, ... from inputTable t1 where t1.INT_col = cast('1' as INT) AND ..
    query = "SELECT * FROM " + inputFile + " WHERE " + Joiner.on(" AND ").join(columnsAndCastsAndComparisons);
    /*
    String query2 = "SELECT Cast( `int_col` AS             INT)             int_col,  " +
        "       Cast( `bigint_col` AS          BIGINT)          bigint_col,  " +
        "       Cast( `decimal9_col` AS        DECIMAL)         decimal9_col,  " +
        "       Cast( `decimal18_col` AS       DECIMAL(18,9))   decimal18_col,  " +
        "       Cast( `decimal28sparse_col` AS DECIMAL(28, 14)) decimal28sparse_col,  " +
        "       Cast( `decimal38sparse_col` AS DECIMAL(38, 19)) decimal38sparse_col,  " +
        "       Cast( `date_col` AS            DATE)            date_col,  " +
        "       Cast( `time_col` AS            TIME)            time_col,  " +
        "       Cast( `timestamp_col` AS TIMESTAMP)             timestamp_col,  " +
        "       Cast( `float4_col` AS FLOAT)                    float4_col,  " +
        "       Cast( `float8_col` AS DOUBLE)                   float8_col,  " +
        "       Cast( `bit_col` AS       BOOLEAN)                     bit_col,  " +
        "       Cast( `varchar_col` AS   VARCHAR(65000))              varchar_col,  " +
        "       Cast( `var16char_col` AS VARCHAR(65000))              var16char_col,  " +
        "       Cast( `varbinary_col` AS VARBINARY(65000))            varbinary_col,  " +
        "       Cast( `intervalyear_col` AS INTERVAL year)            intervalyear_col,  " +
        "       Cast( `intervalday_col` AS INTERVAL day)              intervalday_col  " +
        "FROM   cp.`/parquet/alltypes.json`  " +
        "WHERE  `int_col` = cast( '1' AS                               int)  " +
        "AND    `bigint_col` = cast( '1' AS                            bigint)  " +
//        "AND    `decimal9_col` = cast( '1.0' AS                        decimal)  " +
//        "AND    `decimal18_col` = cast( '123456789.000000000' AS       decimal(18,9))  " +
//        "AND    `decimal28sparse_col` = cast( '123456789.000000000' AS decimal(28, 14))  " +
//        "AND    `decimal38sparse_col` = cast( '123456789.000000000' AS decimal(38, 19))  " +
        "AND    `date_col` = cast( '1995-01-01' AS                     date)  " +
        "AND    `time_col` = cast( '01:00:00' AS                       time)  " +
        "AND    `timestamp_col` = cast( '1995-01-01 01:00:10.000' AS timestamp)  " +
        "AND    `float4_col` = cast( '1' AS float)  " +
        "AND    `float8_col` = cast( '1' AS DOUBLE)  " +
        "AND    `bit_col` = cast( 'false' AS        boolean)  " +
        "AND    `varchar_col` = cast( 'qwerty' AS   varchar(65000))  " +
        "AND    `var16char_col` = cast( 'qwerty' AS varchar(65000))  " +
        "AND    `varbinary_col` = converttonullablevarbinary('qwerty')  " +
        "AND    `intervalyear_col` = converttonullableintervalyear( 'P1Y')  " +
        "AND    `intervalday_col` = converttonullableintervalday( 'P1D' )";
        */
    System.out.println(query);
//    System.out.println(query);
    test("alter system set `store.json.all_text_mode` = true;");
    test(query);
//    test("use dfs.tmp");
//    deleteIfExists("drilltest/parquet_all_types");
//    test("create table parquet_all_types as " + query );



//    runTestAndValidate(Joiner.on(",").join(columnsAndCasts), Joiner.on(",").join(columns), inputFile, "parquet_all_types");
//
//        testBuilder()
//            .unOrdered()
//            .sqlQuery("select cast(BIT_col as boolean) as a FROM cp.`/parquet/alltypes.json` limit 1")
//            .baselineColumns("a")
//            .baselineValues(false)
//            .build().run();

  }

  public String generateCast(String value, TypeProtos.MinorType minorType) {

    String s = "cast( " + value + " as " + Types.getNameOfMinorType(minorType);
    // cast to varchar currently defaults to length 1 unless specified
    // decimal types default to a scale of 0 if not specified
    switch(minorType) {
      case VARCHAR:
      case VAR16CHAR:
      case VARBINARY:
        s += "(65000)";
        break;
      case DECIMAL18:
        s += "(18,9)";
        break;
      case DECIMAL28SPARSE:
        s += "(28, 14)";
        break;
      case DECIMAL38SPARSE:
        s += "(38, 19)";
    }
    s += ") ";
    return s;
  }

  @Test
  public void generateTestFileWithMockScan() throws Exception {

    Path path = new Path("/tmp/drilltest/parquet/all_types");
    if (fs.exists(path)) {
      fs.delete(path, true);
    }

    List<String> mockDataConfigs = new ArrayList();
    for (TypeProtos.MinorType minorType : values()) {
      if (toSkip.contains(minorType)) {
        continue;
      }
      for (DataMode dm : DataMode.values()) {
        mockDataConfigs.add(String.format("{name: \"%s\", type:\"%s\", mode:\"%s\"}", dm.name() + "_" + minorType.name() + "_col", minorType.name(), dm.name()));
      }
    }
    String plan = getFile("parquet/generate_all_types_physical_plan.json");
    plan = plan.replace("REPLACED_IN_TEST", Joiner.on(",").join(mockDataConfigs));
    System.out.println(plan);
    testPhysical(plan);
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
    runTestAndValidate(selection, validationSelection, inputTable, "drill_929");
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

  @Test
  public void testTPCHReadWriteNoDictUncompressed() throws Exception {
    test(String.format("alter session set `%s` = false;", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING));
    test(String.format("alter session set `%s` = 'none'", ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE));
    String inputTable = "cp.`tpch/supplier.parquet`";
    runTestAndValidate("*", "*", inputTable, "supplier_parquet_no_dict_uncompressed");
    test(String.format("alter session set `%s` = true;", ExecConstants.PARQUET_WRITER_ENABLE_DICTIONARY_ENCODING));
    test(String.format("alter session set `%s` = 'snappy'", ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE));
  }

  @Test
  public void testTPCHReadWriteDictGzip() throws Exception {
    test(String.format("alter session set `%s` = 'gzip'", ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE));
    String inputTable = "cp.`tpch/supplier.parquet`";
    runTestAndValidate("*", "*", inputTable, "supplier_parquet_dict_gzip");
    test(String.format("alter session set `%s` = 'snappy'", ExecConstants.PARQUET_WRITER_COMPRESSION_TYPE));
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
      test(String.format("ALTER SESSION SET `%s` = %d", ExecConstants.PARQUET_BLOCK_SIZE, 1*1024*1024));
      String selection = "mi";
      String inputTable = "cp.`customer.json`";
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

  @Test
  public void testBoolean() throws Exception {
    String selection = "true as x, false as y";
    String validateSelection = "x, y";
    String inputTable = "cp.`tpch/region.parquet`";
    runTestAndValidate(selection, validateSelection, inputTable, "region_boolean_parquet");
  }

  @Test //DRILL-2030
  public void testWriterWithStarAndExp() throws Exception {
    String selection = " *, r_regionkey + 1";
    String validateSelection = "r_regionkey, r_name, r_comment, r_regionkey + 1";
    String inputTable = "cp.`tpch/region.parquet`";
    runTestAndValidate(selection, validateSelection, inputTable, "region_star_exp");
  }

  public void compareParquetReadersColumnar(String selection, String table) throws Exception {
    String query = "select " + selection + " from " + table;
    testBuilder()
        .ordered()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery("alter system set `store.parquet.use_new_reader` = false")
        .sqlBaselineQuery(query)
        .optionSettingQueriesForBaseline("alter system set `store.parquet.use_new_reader` = true")
        .build().run();

  }

  public void compareParquetReadersHyperVector(String selection, String table) throws Exception {

    String query = "select " + selection + " from " + table;
    testBuilder()
        .ordered()
        .highPerformanceComparison()
        .sqlQuery(query)
        .optionSettingQueriesForTestQuery("alter system set `store.parquet.use_new_reader` = false")
        .sqlBaselineQuery(query)
        .optionSettingQueriesForBaseline("alter system set `store.parquet.use_new_reader` = true")
        .build().run();
  }

  @Ignore
  @Test
  public void testReadVoter() throws Exception {
    compareParquetReadersHyperVector("*", "dfs.`/tmp/voter.parquet`");
  }

  @Ignore
  @Test
  public void testReadSf_100_supplier() throws Exception {
    compareParquetReadersHyperVector("*", "dfs.`/tmp/sf100_supplier.parquet`");
  }

  @Ignore
  @Test
  public void testParquetRead_checkNulls_NullsFirst() throws Exception {
    compareParquetReadersColumnar("*", "dfs.`/tmp/parquet_with_nulls_should_sum_100000_nulls_first.parquet`");
  }

  @Ignore
  @Test
  public void testParquetRead_checkNulls() throws Exception {
    compareParquetReadersColumnar("*", "dfs.`/tmp/parquet_with_nulls_should_sum_100000.parquet`");
  }

  @Ignore
  @Test
  public void test958_sql() throws Exception {
    compareParquetReadersHyperVector("ss_ext_sales_price", "dfs.`/tmp/store_sales`");
  }

  @Ignore
  @Test
  public void testReadSf_1_supplier() throws Exception {
    compareParquetReadersHyperVector("*", "dfs.`/tmp/orders_part-m-00001.parquet`");
  }

  @Ignore
  @Test
  public void test958_sql_all_columns() throws Exception {
    compareParquetReadersHyperVector("*",  "dfs.`/tmp/store_sales`");
    compareParquetReadersHyperVector("ss_addr_sk, ss_hdemo_sk", "dfs.`/tmp/store_sales`");
    // TODO - Drill 1388 - this currently fails, but it is an issue with project, not the reader, pulled out the physical plan
    // removed the unneeded project in the plan and ran it against both readers, they outputs matched
//    compareParquetReadersHyperVector("pig_schema,ss_sold_date_sk,ss_item_sk,ss_cdemo_sk,ss_addr_sk, ss_hdemo_sk",
//        "dfs.`/tmp/store_sales`");
  }

  @Ignore
  @Test
  public void testDrill_1314() throws Exception {
    compareParquetReadersColumnar("l_partkey ", "dfs.`/tmp/drill_1314.parquet`");
  }

  @Ignore
  @Test
  public void testDrill_1314_all_columns() throws Exception {
    compareParquetReadersHyperVector("*", "dfs.`/tmp/drill_1314.parquet`");
    compareParquetReadersColumnar("l_orderkey,l_partkey,l_suppkey,l_linenumber, l_quantity, l_extendedprice,l_discount,l_tax",
        "dfs.`/tmp/drill_1314.parquet`");
  }

  @Ignore
  @Test
  public void testParquetRead_checkShortNullLists() throws Exception {
    compareParquetReadersColumnar("*", "dfs.`/tmp/short_null_lists.parquet`");
  }

  @Ignore
  @Test
  public void testParquetRead_checkStartWithNull() throws Exception {
    compareParquetReadersColumnar("*", "dfs.`/tmp/start_with_null.parquet`");
  }

  @Ignore
  @Test
  public void testParquetReadWebReturns() throws Exception {
    compareParquetReadersColumnar("wr_returning_customer_sk", "dfs.`/tmp/web_returns`");
  }

  public void deleteIfExists(String outputFile) throws IOException {
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

    testBuilder()
        .unOrdered()
        .sqlQuery(query)
        .sqlBaselineQuery(validateQuery);
  }

}
