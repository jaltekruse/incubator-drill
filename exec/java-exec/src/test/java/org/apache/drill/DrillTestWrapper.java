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
import org.apache.drill.exec.HyperVectorValueIterator;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * An object to encapsulate the options for a Drill unit test.
 *
 * To construct an instance easily, look at the TestBuilder class.
 */
public class DrillTestWrapper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);

  // TODO - when in JSON, read baseline in all text mode to avoid precision loss for decimal values
  private static boolean VERBOSE_DEBUG = false;

  private TestBuilder testBuilder;
  // test query to run
  private String query;
  // The type of query provided
  private UserBitShared.QueryType queryType;
  // The type of query provided for the baseline
  private UserBitShared.QueryType baselineQueryType;
  // should ordering be enforced in the baseline chectbuilder
  private boolean ordered;
  // TODO - implement this
  private boolean approximateEquality;
  private BufferAllocator allocator;
  // queries to run before the baseline or test queries, can be used to set options
  private String baselineOptionSettingQueries;
  private String testOptionSettingQueries;
  // two different methods are available for comparing ordered results, the default reads all of the records
  // into giant lists of objects, like one giant on-heap batch of 'vectors'
  // this flag enables the other approach which iterates through a hyper batch for the test query results and baseline
  // while this does work faster and use less memory, it can be harder to debug as all of the elements are not in a
  // single list
  private boolean highPerformanceComparison;

  public DrillTestWrapper(TestBuilder testBuilder, BufferAllocator allocator, String query, QueryType queryType,
                          String baselineOptionSettingQueries, String testOptionSettingQueries,
                          String baselineQuery, QueryType baselineQueryType, boolean ordered, boolean approximateEquality,
                          boolean highPerformanceComparison) {
    this.testBuilder = testBuilder;
    this.allocator = allocator;
    this.query = query;
    this.queryType = queryType;
    this.baselineQueryType = baselineQueryType;
    this.ordered = ordered;
    this.approximateEquality = approximateEquality;
    this.baselineOptionSettingQueries = baselineOptionSettingQueries;
    this.testOptionSettingQueries = testOptionSettingQueries;
    this.highPerformanceComparison = highPerformanceComparison;
  }

  public void run() throws Exception {
    if (ordered) {
      compareOrderedResults();
    } else {
      compareUnorderedResults();
    }
  }

  private BufferAllocator getAllocator() {
    return allocator;
  }

  private void compareHyperVectors(Map<String, HyperVectorValueIterator> expectedRecords,
                                         Map<String, HyperVectorValueIterator> actualRecords) throws Exception {
    for (String s : expectedRecords.keySet()) {
      assertEquals(expectedRecords.get(s).getTotalRecords(), actualRecords.get(s).getTotalRecords());
      HyperVectorValueIterator expectedValues = expectedRecords.get(s);
      HyperVectorValueIterator actualValues = actualRecords.get(s);
      int i = 0;
      while (expectedValues.hasNext()) {
        compareValues(expectedValues.next(), actualValues.next(), i, s);
        i++;
      }
    }
    for (HyperVectorValueIterator hvi : expectedRecords.values()) {
      for (ValueVector vv : hvi.getHyperVector().getValueVectors()) {
        vv.clear();
      }
    }
    for (HyperVectorValueIterator hvi : actualRecords.values()) {
      for (ValueVector vv : hvi.getHyperVector().getValueVectors()) {
        vv.clear();
      }
    }
  }

  private void compareMergedVectors(Map<String, List> expectedRecords, Map<String, List> actualRecords) throws Exception {
    for (String s : expectedRecords.keySet()) {
      assertNotNull("Expected column '" + s + "' not found.", actualRecords.get(s));
      assertEquals(expectedRecords.get(s).size(), actualRecords.get(s).size());
      List expectedValues = expectedRecords.get(s);
      List actualValues = actualRecords.get(s);
      for (int i = 0; i < expectedValues.size(); i++) {
        compareValues(expectedValues.get(i), actualValues.get(i), i, s);
      }
    }
  }

  private Map<String, HyperVectorValueIterator> addToHyperVectorMap(List<QueryResultBatch> records, RecordBatchLoader loader,
                                                                      BatchSchema schema) throws SchemaChangeException, UnsupportedEncodingException {
    // TODO - this does not handle schema changes
    Map<String, HyperVectorValueIterator> combinedVectors = new HashMap();

    long totalRecords = 0;
    QueryResultBatch batch;
    int size = records.size();
    for (int i = 0; i < size; i++) {
      batch = records.get(i);
      loader = new RecordBatchLoader(getAllocator());
      loader.load(batch.getHeader().getDef(), batch.getData());
      logger.debug("reading batch with " + loader.getRecordCount() + " rows, total read so far " + totalRecords);
      totalRecords += loader.getRecordCount();
      for (VectorWrapper w : loader) {
        String field = w.getField().toExpr();
        if ( ! combinedVectors.containsKey(field)) {
          MaterializedField mf = w.getField();
          ValueVector[] vvList = (ValueVector[]) Array.newInstance(mf.getValueClass(), 1);
          vvList[0] = w.getValueVector();
          combinedVectors.put(mf.getPath().toExpr(), new HyperVectorValueIterator(mf, new HyperVectorWrapper(mf,
              vvList)));
        } else {
          combinedVectors.get(field).getHyperVector().addVector(w.getValueVector());
        }

      }
    }
    for (HyperVectorValueIterator hvi : combinedVectors.values()) {
      hvi.determineTotalSize();
    }
    return combinedVectors;
  }

  /**
   * Only use this method if absolutely needed. There are utility methods to compare results of single queries.
   * The current use case for exposing this is setting session or system options between the test and verification
   * queries.
   *
   * TODO - evaluate adding an interface to allow setting session and system options before running queries
   * @param records
   * @param loader
   * @param schema
   * @return
   * @throws SchemaChangeException
   * @throws UnsupportedEncodingException
   */
   private Map<String, List> addToCombinedVectorResults(List<QueryResultBatch> records, RecordBatchLoader loader,
                                                         BatchSchema schema) throws SchemaChangeException, UnsupportedEncodingException {
    // TODO - this does not handle schema changes
    Map<String, List> combinedVectors = new HashMap();

    long totalRecords = 0;
    QueryResultBatch batch;
    int size = records.size();
    for (int i = 0; i < size; i++) {
      batch = records.get(0);
      loader.load(batch.getHeader().getDef(), batch.getData());
      if (schema == null) {
        schema = loader.getSchema();
        for (MaterializedField mf : schema) {
          combinedVectors.put(mf.getPath().toExpr(), new ArrayList());
        }
      }
      logger.debug("reading batch with " + loader.getRecordCount() + " rows, total read so far " + totalRecords);
      totalRecords += loader.getRecordCount();
      for (VectorWrapper w : loader) {
        String field = w.getField().toExpr();
        for (int j = 0; j < loader.getRecordCount(); j++) {
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
          }
          combinedVectors.get(field).add(obj);
        }
      }
      records.remove(0);
      batch.release();
      loader.clear();
    }
    return combinedVectors;
  }

  /**
   * Use this method only if necessary to validate one query against another. If you are just validating against a
   * baseline file use one of the simpler interfaces that will write the validation query for you.
   *
   * @throws Exception
   */
  protected void compareUnorderedResults() throws Exception {
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    BatchSchema schema = null;

    BaseTestQuery.test(testOptionSettingQueries);
    List<QueryResultBatch> expected = BaseTestQuery.testRunAndReturn(queryType, query);

    addTypeInfoIfMissing(expected.get(0), testBuilder);

    List<Map> expectedRecords = new ArrayList<>();
    addToMaterializedResults(expectedRecords, expected, loader, schema);

    BaseTestQuery.test(baselineOptionSettingQueries);
    List<QueryResultBatch> results = BaseTestQuery.testRunAndReturn(baselineQueryType, testBuilder.getValidationQuery());
    List<Map> actualRecords = new ArrayList<>();
    addToMaterializedResults(actualRecords, results, loader, schema);

    compareResults(expectedRecords, actualRecords);
    cleanupBatches(expected, results);
  }

  /**
   * Use this method only if necessary to validate one query against another. If you are just validating against a
   * baseline file use one of the simpler interfaces that will write the validation query for you.
   *
   * @throws Exception
   */
  protected void compareOrderedResults() throws Exception {
    if (highPerformanceComparison) {
      compareResultsHyperVector();
    } else {
      compareMergedOnHeapVectors();
    }
  }

  public void compareMergedOnHeapVectors() throws Exception {
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    BatchSchema schema = null;

    BaseTestQuery.test(testOptionSettingQueries);
    List<QueryResultBatch> results = BaseTestQuery.testRunAndReturn(queryType, query);
    addTypeInfoIfMissing(results.get(0), testBuilder);

    Map<String, List> actualSuperVectors = addToCombinedVectorResults(results, loader, schema);

    BaseTestQuery.test(baselineOptionSettingQueries);
    List<QueryResultBatch> expected = BaseTestQuery.testRunAndReturn(baselineQueryType, testBuilder.getValidationQuery());
    Map<String, List> expectedSuperVectors = addToCombinedVectorResults(expected, loader, schema);

    compareMergedVectors(expectedSuperVectors, actualSuperVectors);

    cleanupBatches(expected, results);
  }

  public void compareResultsHyperVector() throws Exception {
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    BatchSchema schema = null;

    BaseTestQuery.test(testOptionSettingQueries);
    List<QueryResultBatch> results = BaseTestQuery.testRunAndReturn(queryType, query);
    addTypeInfoIfMissing(results.get(0), testBuilder);

    Map<String, HyperVectorValueIterator> actualSuperVectors = addToHyperVectorMap(results, loader, schema);

    BaseTestQuery.test(baselineOptionSettingQueries);
    List<QueryResultBatch> expected = BaseTestQuery.testRunAndReturn(baselineQueryType, testBuilder.getValidationQuery());

    Map<String, HyperVectorValueIterator> expectedSuperVectors = addToHyperVectorMap(expected, loader, schema);

    compareHyperVectors(expectedSuperVectors, actualSuperVectors);
    cleanupBatches(results, expected);
  }

  private void addTypeInfoIfMissing(QueryResultBatch batch, TestBuilder testBuilder) {
    if (! testBuilder.typeInfoSet()) {
      Map<SchemaPath, TypeProtos.MinorType> typeMap = getTypeMapFromBatch(batch);
      testBuilder.baselineTypes(typeMap);
    }

  }

  private Map<SchemaPath, TypeProtos.MinorType> getTypeMapFromBatch(QueryResultBatch batch) {
    Map<SchemaPath, TypeProtos.MinorType> typeMap = new HashMap();
    for (int i = 0; i < batch.getHeader().getDef().getFieldCount(); i++) {
      typeMap.put(MaterializedField.create(batch.getHeader().getDef().getField(i)).getPath(),
          batch.getHeader().getDef().getField(i).getMajorType().getMinorType());
    }
    return typeMap;
  }

  private void cleanupBatches(List<QueryResultBatch>... results) {
    for (List<QueryResultBatch> resultList : results ) {
      for (QueryResultBatch result : resultList) {
        result.release();
      }
    }
  }

  protected void addToMaterializedResults(List<Map> materializedRecords,  List<QueryResultBatch> records, RecordBatchLoader loader,
                                          BatchSchema schema) throws SchemaChangeException, UnsupportedEncodingException {
    long totalRecords = 0;
    QueryResultBatch batch;
    int size = records.size();
    for (int i = 0; i < size; i++) {
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

  public static void compareValues(Object expected, Object actual, int counter, String column) throws Exception {

    if (expected == null) {
      if (actual == null) {
        if (VERBOSE_DEBUG) {
          logger.debug("(1) at position " + counter + " column '" + column + "' matched value:  " + expected );
        }
        return;
      } else {
        throw new Exception("at position " + counter + " column '" + column + "' mismatched values, expected: "
            + expected + "(" + expected.getClass().getSimpleName() + ") but received " + actual + "(" + actual.getClass().getSimpleName() + ")");
      }
    }
    if (actual == null) {
      throw new Exception("unexpected null at position " + counter + " column '" + column + "' should have been:  " + expected);
    }
    if (actual instanceof byte[]) {
      if ( ! Arrays.equals((byte[]) expected, (byte[]) actual)) {
        throw new Exception("at position " + counter + " column '" + column + "' mismatched values, expected: "
            + new String((byte[])expected, "UTF-8") + " but received " + new String((byte[])actual, "UTF-8"));
      } else {
        if (VERBOSE_DEBUG) {
          logger.debug("at position " + counter + " column '" + column + "' matched value " + new String((byte[])expected, "UTF-8"));
        }
        return;
      }
    }
    if (!expected.equals(actual)) {
      throw new Exception("at position " + counter + " column '" + column + "' mismatched values, expected: "
          + expected + "(" + expected.getClass().getSimpleName() + ") but received " + actual + "(" + actual.getClass().getSimpleName() + ")");
    } else {
      if (VERBOSE_DEBUG) {
        logger.debug("at position " + counter + " column '" + column + "' matched value:  " + expected );
      }
    }
  }

  /**
   * Compare two result sets, ignoring ordering.
   *
   * @param expectedRecords
   * @param actualRecords
   * @throws Exception
   */
  private void compareResults(List<Map> expectedRecords, List<Map> actualRecords) throws Exception {

    assertEquals("Different number of records returned", expectedRecords.size(), actualRecords.size());

    String missing = "";
    for (Map<String, Object> record : expectedRecords) {
      if ( ! actualRecords.remove(record)) {
        // TODO - add some kind of row major record serialization, append to the 'missing' string
      }
    }
    logger.debug(missing);
    System.out.println(missing);
    assertEquals(0, actualRecords.size());
  }

}
