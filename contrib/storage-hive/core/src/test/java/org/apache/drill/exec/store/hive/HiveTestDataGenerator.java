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

package org.apache.drill.exec.store.hive;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.google.common.collect.Maps;

import static org.apache.drill.BaseTestQuery.getTempDir;
import static org.apache.drill.exec.hive.HiveTestUtilities.executeQuery;

public class HiveTestDataGenerator {
  private static final String HIVE_TEST_PLUGIN_NAME = "hive";
  private static HiveTestDataGenerator instance;

  private final String dbDir;
  private final String whDir;
  private final Map<String, String> config;

  /**
   * Get an instance of the test data generator for Hive. If no instance has been created this
   * will prompt the generation of test tables. A DrillClient is passed to enable generation of
   * parquet files.
   *
   * @return
   * @throws Exception
   */
  public static synchronized HiveTestDataGenerator getInstance() throws Exception {
    if (instance == null) {
      final String dbDir = getTempDir("metastore_db");
      final String whDir = getTempDir("warehouse");

      instance = new HiveTestDataGenerator(dbDir, whDir);
      instance.generateTestData();
    }

    return instance;
  }

  private HiveTestDataGenerator(final String dbDir, final String whDir) {
    this.dbDir = dbDir;
    this.whDir = whDir;

    config = Maps.newHashMap();
    config.put("hive.metastore.uris", "");
    config.put("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:;databaseName=%s;create=true", dbDir));
    config.put("hive.metastore.warehouse.dir", whDir);
    config.put(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
  }

  /**
   * Add Hive test storage plugin to the given plugin registry.
   * @throws Exception
   */
  public void addHiveTestPlugin(final StoragePluginRegistry pluginRegistry) throws Exception {
    HiveStoragePluginConfig pluginConfig = new HiveStoragePluginConfig(config);
    pluginConfig.setEnabled(true);

    pluginRegistry.createOrUpdate(HIVE_TEST_PLUGIN_NAME, pluginConfig, true);
  }

  /**
   * Update the current HiveStoragePlugin in given plugin registry with given <i>configOverride</i>.
   *
   * @param configOverride
   * @throws DrillException if fails to update or no Hive plugin currently exists in given plugin registry.
   */
  public void updatePluginConfig(final StoragePluginRegistry pluginRegistry, Map<String, String> configOverride)
      throws DrillException {
    HiveStoragePlugin storagePlugin = (HiveStoragePlugin) pluginRegistry.getPlugin(HIVE_TEST_PLUGIN_NAME);
    if (storagePlugin == null) {
      throw new DrillException(
          "Hive test storage plugin doesn't exist. Add a plugin using addHiveTestPlugin()");
    }

    HiveStoragePluginConfig newPluginConfig = storagePlugin.getConfig();
    newPluginConfig.getHiveConfigOverride().putAll(configOverride);

    pluginRegistry.createOrUpdate(HIVE_TEST_PLUGIN_NAME, newPluginConfig, true);
  }

  /**
   * Delete the Hive test plugin from registry.
   */
  public void deleteHiveTestPlugin(final StoragePluginRegistry pluginRegistry) {
    pluginRegistry.deletePlugin(HIVE_TEST_PLUGIN_NAME);
  }

  private void generateTestData() throws Exception {

    HiveConf conf = new HiveConf(SessionState.class);

    conf.set("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:;databaseName=%s;create=true", dbDir));
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    conf.set("hive.metastore.warehouse.dir", whDir);
    conf.set("mapred.job.tracker", "local");
    conf.set(ConfVars.SCRATCHDIR.varname,  getTempDir("scratch_dir"));
    conf.set(ConfVars.LOCALSCRATCHDIR.varname, getTempDir("local_scratch_dir"));

    SessionState ss = new SessionState(conf);
    SessionState.start(ss);
    Driver hiveDriver = new Driver(conf);

    // generate (key, value) test data
    String testDataFile = generateTestDataFile();

    // Create a (key, value) schema table with Text SerDe which is available in hive-serdes.jar
    executeQuery(hiveDriver, "CREATE TABLE IF NOT EXISTS default.kv(key INT, value STRING) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
    executeQuery(hiveDriver, "LOAD DATA LOCAL INPATH '" + testDataFile + "' OVERWRITE INTO TABLE default.kv");

    // Create a (key, value) schema table in non-default database with RegexSerDe which is available in hive-contrib.jar
    // Table with RegExSerde is expected to have columns of STRING type only.
    executeQuery(hiveDriver, "CREATE DATABASE IF NOT EXISTS db1");
    executeQuery(hiveDriver, "CREATE TABLE db1.kv_db1(key STRING, value STRING) " +
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe' " +
        "WITH SERDEPROPERTIES (" +
        "  \"input.regex\" = \"([0-9]*), (.*_[0-9]*)\", " +
        "  \"output.format.string\" = \"%1$s, %2$s\"" +
        ") ");
    executeQuery(hiveDriver, "INSERT INTO TABLE db1.kv_db1 SELECT * FROM default.kv");

    // Create an Avro format based table backed by schema in a separate file
    final String avroCreateQuery = String.format("CREATE TABLE db1.avro " +
        "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
        "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
        "TBLPROPERTIES ('avro.schema.url'='file:///%s')",
        BaseTestQuery.getPhysicalFileFromResource("avro_test_schema.json").replace('\\', '/'));

    executeQuery(hiveDriver, avroCreateQuery);
    executeQuery(hiveDriver, "INSERT INTO TABLE db1.avro SELECT * FROM default.kv");

    executeQuery(hiveDriver, "USE default");

    // create a table with no data
    executeQuery(hiveDriver, "CREATE TABLE IF NOT EXISTS empty_table(a INT, b STRING)");
    // delete the table location of empty table
    File emptyTableLocation = new File(whDir, "empty_table");
    if (emptyTableLocation.exists()) {
      FileUtils.forceDelete(emptyTableLocation);
    }

    // create a Hive table that has columns with data types which are supported for reading in Drill.
    testDataFile = generateAllTypesDataFile();
    executeQuery(hiveDriver,
        "CREATE TABLE IF NOT EXISTS readtest (" +
        "  binary_field BINARY," +
        "  boolean_field BOOLEAN," +
        "  tinyint_field TINYINT," +
        "  decimal0_field DECIMAL," +
        "  decimal9_field DECIMAL(6, 2)," +
        "  decimal18_field DECIMAL(15, 5)," +
        "  decimal28_field DECIMAL(23, 1)," +
        "  decimal38_field DECIMAL(30, 3)," +
        "  double_field DOUBLE," +
        "  float_field FLOAT," +
        "  int_field INT," +
        "  bigint_field BIGINT," +
        "  smallint_field SMALLINT," +
        "  string_field STRING," +
        "  varchar_field VARCHAR(50)," +
        "  timestamp_field TIMESTAMP," +
        "  date_field DATE" +
        ") PARTITIONED BY (" +
        "  binary_part BINARY," +
        "  boolean_part BOOLEAN," +
        "  tinyint_part TINYINT," +
        "  decimal0_part DECIMAL," +
        "  decimal9_part DECIMAL(6, 2)," +
        "  decimal18_part DECIMAL(15, 5)," +
        "  decimal28_part DECIMAL(23, 1)," +
        "  decimal38_part DECIMAL(30, 3)," +
        "  double_part DOUBLE," +
        "  float_part FLOAT," +
        "  int_part INT," +
        "  bigint_part BIGINT," +
        "  smallint_part SMALLINT," +
        "  string_part STRING," +
        "  varchar_part VARCHAR(50)," +
        "  timestamp_part TIMESTAMP," +
        "  date_part DATE" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
        "TBLPROPERTIES ('serialization.null.format'='') "
    );

    // Add a partition to table 'readtest'
    executeQuery(hiveDriver,
        "ALTER TABLE readtest ADD IF NOT EXISTS PARTITION ( " +
        "  binary_part='binary', " +
        "  boolean_part='true', " +
        "  tinyint_part='64', " +
        "  decimal0_part='36.9', " +
        "  decimal9_part='36.9', " +
        "  decimal18_part='3289379872.945645', " +
        "  decimal28_part='39579334534534.35345', " +
        "  decimal38_part='363945093845093890.9', " +
        "  double_part='8.345', " +
        "  float_part='4.67', " +
        "  int_part='123456', " +
        "  bigint_part='234235', " +
        "  smallint_part='3455', " +
        "  string_part='string', " +
        "  varchar_part='varchar', " +
        "  timestamp_part='2013-07-05 17:01:00', " +
        "  date_part='2013-07-05' " +
        ")"
    );

    // Load data into table 'readtest'
    executeQuery(hiveDriver,
        String.format("LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE default.readtest PARTITION (" +
        "  binary_part='binary', " +
        "  boolean_part='true', " +
        "  tinyint_part='64', " +
        "  decimal0_part='36.9', " +
        "  decimal9_part='36.9', " +
        "  decimal18_part='3289379872.945645', " +
        "  decimal28_part='39579334534534.35345', " +
        "  decimal38_part='363945093845093890.9', " +
        "  double_part='8.345', " +
        "  float_part='4.67', " +
        "  int_part='123456', " +
        "  bigint_part='234235', " +
        "  smallint_part='3455', " +
        "  string_part='string', " +
        "  varchar_part='varchar', " +
        "  timestamp_part='2013-07-05 17:01:00', " +
        "  date_part='2013-07-05'" +
        ")",
            testDataFile)
    );

//    executeQuery(hiveDriver,
//        "CREATE EXTERNAL TABLE IF NOT EXISTS readtest_parquet (" +
//            "  boolean_field BOOLEAN," +
//            "  tinyint_field TINYINT," +
//            "  double_field DOUBLE," +
//            "  float_field FLOAT," +
//            "  int_field INT," +
//            "  bigint_field BIGINT," +
//            "  smallint_field SMALLINT," +
//            "  string_field STRING" +
//            ") STORED as PARQUET location '/Users/jaltekruse/test_data_drill/par_hive_types'"
//    );
//            "AS SELECT " +
//        "boolean_field, tinyint_field, double_field, float_field, int_field, bigint_field, smallint_field, string_field " +
//        "FROM readtest"
//            /Users/jaltekruse/test_data_drill/par_hive_types

    /*
    // create a table that has all Hive types. This is to test how hive tables metadata is populated in
    // Drill's INFORMATION_SCHEMA.
    executeQuery(hiveDriver,
        "CREATE TABLE IF NOT EXISTS infoschematest(" +
        "booleanType BOOLEAN, " +
        "tinyintType TINYINT, " +
        "smallintType SMALLINT, " +
        "intType INT, " +
        "bigintType BIGINT, " +
        "floatType FLOAT, " +
        "doubleType DOUBLE, " +
        "dateType DATE, " +
        "timestampType TIMESTAMP, " +
        "binaryType BINARY, " +
        "decimalType DECIMAL(38, 2), " +
        "stringType STRING, " +
        "varCharType VARCHAR(20), " +
        "listType ARRAY<STRING>, " +
        "mapType MAP<STRING,INT>, " +
        "structType STRUCT<sint:INT,sboolean:BOOLEAN,sstring:STRING>, " +
        "uniontypeType UNIONTYPE<int, double, array<string>>)"
    );
    */

    executeQuery(hiveDriver, "SHOW CREATE TABLE readtest");

    executeQuery(hiveDriver, "SHOW PARTITIONS readtest");

    executeQuery(hiveDriver, "select * from readtest");

    executeQuery(hiveDriver, "" +
        "CREATE TABLE parquet_text_mixed_fileformat (" +
        "  boolean_field BOOLEAN," +
        "  tinyint_field TINYINT," +
        "  double_field DOUBLE," +
        "  float_field FLOAT," +
        "  int_field INT," +
        "  bigint_field BIGINT," +
        "  smallint_field SMALLINT," +
        "  string_field STRING" +
        ") PARTITIONED BY (" +
        "  boolean_part BOOLEAN," +
        "  tinyint_part TINYINT," +
        "  double_part DOUBLE," +
        "  float_part FLOAT," +
        "  int_part INT," +
        "  bigint_part BIGINT," +
        "  smallint_part SMALLINT," +
        "  string_part STRING" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");

//        "CREATE TABLE parquet_mixed_fileformat  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE " +

    // Add a partition to table 'readtest'
    executeQuery(hiveDriver,
        "ALTER TABLE parquet_text_mixed_fileformat ADD IF NOT EXISTS PARTITION ( " +
            "  boolean_part='true', " +
            "  tinyint_part='64', " +
            "  double_part='8.345', " +
            "  float_part='4.67', " +
            "  int_part='123456', " +
            "  bigint_part='234235', " +
            "  smallint_part='3455', " +
            "  string_part='string' " +
            ")"
    );

    executeQuery(hiveDriver,
        "INSERT INTO TABLE parquet_text_mixed_fileformat " +
            "PARTITION (" +
            "  boolean_part='true', " +
            "  tinyint_part='64', " +
            "  double_part='8.345', " +
            "  float_part='4.67', " +
            "  int_part='123456', " +
            "  bigint_part='234235', " +
            "  smallint_part='3455', " +
            "  string_part='string'" +
            ") " +
            "SELECT " +
            "boolean_field, tinyint_field, double_field, float_field, int_field, bigint_field, smallint_field, string_field " +
            "FROM readtest ");

    executeQuery(hiveDriver, "select * from parquet_text_mixed_fileformat ");

    executeQuery(hiveDriver, "SHOW CREATE TABLE parquet_text_mixed_fileformat ");
//    executeQuery(hiveDriver, "ALTER TABLE parquet_mixed_fileformat set SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'");

  // TODO - re-enable to actually test mixed format, for now this is just testing partitions with TEXT
//    executeQuery(hiveDriver, "ALTER TABLE parquet_text_mixed_fileformat " +
//        "SET FILEFORMAT " +
//        "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
//        "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " +
//        "SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'");

//    executeQuery(hiveDriver,
//        "INSERT INTO TABLE parquet_text_mixed_fileformat  " +
//            "SELECT " +
//            "boolean_field, tinyint_field, double_field, float_field, int_field, bigint_field, smallint_field, string_field " +
//            "FROM readtest ");

    executeQuery(hiveDriver,
        "INSERT INTO TABLE parquet_text_mixed_fileformat " +
            "PARTITION (" +
            "  boolean_part='true', " +
//            changed this from 64
            "  tinyint_part='63', " +
            "  double_part='8.345', " +
            "  float_part='4.67', " +
            "  int_part='123456', " +
            "  bigint_part='234235', " +
            "  smallint_part='3455', " +
            "  string_part='string'" +
            ") " +
            "SELECT " +
            "boolean_field, tinyint_field, double_field, float_field, int_field, bigint_field, smallint_field, string_field " +
            "FROM readtest ");

    File file = new File(testDataFile);
    String parentDirectory = file.getParentFile().toString();

    // External partition
    executeQuery(hiveDriver, String.format("ALTER TABLE parquet_text_mixed_fileformat ADD " +
            "PARTITION (" +
            "  boolean_part='true', " +
//            changed this from 64
            "  tinyint_part='62', " +
            "  double_part='8.345', " +
            "  float_part='4.67', " +
            "  int_part='123456', " +
            "  bigint_part='234235', " +
            "  smallint_part='3455', " +
            "  string_part='string'" +
            ") " +
        "location '%s'", parentDirectory));

    // External Table
    // Modified from : http://stackoverflow.com/questions/15271061/is-it-possible-to-import-data-into-hive-table-without-copying-the-data/22170468#22170468
//    executeQuery(hiveDriver, "create EXTERNAL TABLE IF NOT EXISTS test_table (testcol string) " +
//                             "PARTITIONED BY (year INT,month INT,day INT) row format delimited fields terminated by ','");

//    executeQuery(hiveDriver, "ALTER table TestTable partition (year='2014',month='2',day='17') " +
//                             "location 'hdfs://localhost:8020/data/2014/2/17/'");

    // create a Hive view to test how its metadata is populated in Drill's INFORMATION_SCHEMA
    executeQuery(hiveDriver, "CREATE VIEW IF NOT EXISTS hiveview AS SELECT * FROM kv");

    // Generate data with date and timestamp data type
    String testDateDataFile = generateTestDataFileWithDate();

    // create partitioned hive table to test partition pruning
    executeQuery(hiveDriver,
        "CREATE TABLE IF NOT EXISTS default.partition_pruning_test(a DATE, b TIMESTAMP) "+
        "partitioned by (c int, d int, e int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
    executeQuery(hiveDriver,
        String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default.partition_pruning_test partition(c=1, d=1, e=1)", testDateDataFile));
    executeQuery(hiveDriver,
        String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default.partition_pruning_test partition(c=1, d=1, e=2)", testDateDataFile));
    executeQuery(hiveDriver,
        String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default.partition_pruning_test partition(c=1, d=2, e=1)", testDateDataFile));
    executeQuery(hiveDriver,
        String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default.partition_pruning_test partition(c=1, d=1, e=2)", testDateDataFile));
    executeQuery(hiveDriver,
        String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default.partition_pruning_test partition(c=2, d=1, e=1)", testDateDataFile));
    executeQuery(hiveDriver,
        String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default.partition_pruning_test partition(c=2, d=1, e=2)", testDateDataFile));
    executeQuery(hiveDriver,
        String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default.partition_pruning_test partition(c=2, d=3, e=1)", testDateDataFile));
    executeQuery(hiveDriver,
        String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default.partition_pruning_test partition(c=2, d=3, e=2)", testDateDataFile));

    ss.close();
  }

  private File getTempFile() throws Exception {
    // Generate temp files in folders so that they are isolated from one another.
    // This allows for the parent directory to be extracted and provided as the
    // location of an external table or partition in Hive (as we can safely assume
    // the directory will only contain this one file we are putting in it)
    File f = java.nio.file.Files.createTempDirectory("drill-hive-test").toFile();
    return new File(f, "drill-hive-test.txt");
  }

  private String generateTestDataFile() throws Exception {
    final File file = getTempFile();
    PrintWriter printWriter = new PrintWriter(file);
    for (int i=1; i<=5; i++) {
      printWriter.println (String.format("%d, key_%d", i, i));
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateTestDataFileWithDate() throws Exception {
    final File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    for (int i=1; i<=5; i++) {
      Date date = new Date(System.currentTimeMillis());
      Timestamp ts = new Timestamp(System.currentTimeMillis());
      printWriter.println (String.format("%s,%s", date.toString(), ts.toString()));
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateAllTypesDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    printWriter.println("YmluYXJ5ZmllbGQ=,false,34,65.99,2347.923,2758725827.9999,29375892739852.7689," +
        "89853749534593985.7834783,8.345,4.67,123456,234235,3455,stringfield,varcharfield," +
        "2013-07-05 17:01:00,2013-07-05");
    printWriter.println(",,,,,,,,,,,,,,,,");
    printWriter.close();

    return file.getPath();
  }
}
