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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.PrintingResultsListener;
import org.apache.drill.exec.client.QuerySubmitter;
import org.apache.drill.exec.client.QuerySubmitter.Format;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import static org.junit.Assert.assertEquals;

public class BaseTestQuery extends ExecTest{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);

  private int[] columnWidths = new int[] { 8 };
  private static final boolean VERBOSE_DEBUG = false;

  private static final String ENABLE_FULL_CACHE = "drill.exec.test.use-full-cache";

  @SuppressWarnings("serial")
  private static final Properties TEST_CONFIGURATIONS = new Properties() {
    {
      put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
    }
  };

  public final TestRule resetWatcher = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      try {
        resetClientAndBit();
      } catch (Exception e1) {
        throw new RuntimeException("Failure while resetting client.", e1);
      }
    }
  };

  protected static DrillClient client;
  protected static Drillbit bit;
  protected static RemoteServiceSet serviceSet;
  protected static DrillConfig config;
  protected static QuerySubmitter submitter = new QuerySubmitter();
  protected static BufferAllocator allocator;

  static void resetClientAndBit() throws Exception{
    closeClient();
    openClient();
  }

  @BeforeClass
  public static void openClient() throws Exception{
    config = DrillConfig.create(TEST_CONFIGURATIONS);
    allocator = new TopLevelAllocator(config);
    if (config.hasPath(ENABLE_FULL_CACHE) && config.getBoolean(ENABLE_FULL_CACHE)) {
      serviceSet = RemoteServiceSet.getServiceSetWithFullCache(config, allocator);
    } else {
      serviceSet = RemoteServiceSet.getLocalServiceSet();
    }
    bit = new Drillbit(config, serviceSet);
    bit.run();
    client = new DrillClient(config, serviceSet.getCoordinator());
    client.connect();
    List<QueryResultBatch> results = client.runQuery(QueryType.SQL, String.format("alter session set `%s` = 2", ExecConstants.MAX_WIDTH_PER_NODE_KEY));
    for (QueryResultBatch b : results) {
      b.release();
    }
  }



  protected BufferAllocator getAllocator() {
    return allocator;
  }

  @AfterClass
  public static void closeClient() throws IOException{
    if (client != null) {
      client.close();
    }
    if (bit != null) {
      bit.close();
    }
    if(serviceSet != null) {
      serviceSet.close();
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  protected void runSQL(String sql) throws Exception {
    SilentListener listener = new SilentListener();
    testWithListener(QueryType.SQL, sql, listener);
    listener.waitForCompletion();
  }

  protected List<QueryResultBatch> testSqlWithResults(String sql) throws Exception{
    return testRunAndReturn(QueryType.SQL, sql);
  }

  protected List<QueryResultBatch> testLogicalWithResults(String logical) throws Exception{
    return testRunAndReturn(QueryType.LOGICAL, logical);
  }

  protected List<QueryResultBatch> testPhysicalWithResults(String physical) throws Exception{
    return testRunAndReturn(QueryType.PHYSICAL, physical);
  }

  protected List<QueryResultBatch>  testRunAndReturn(QueryType type, String query) throws Exception{
    query = query.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    return client.runQuery(type, query);
  }

  protected int testRunAndPrint(QueryType type, String query) throws Exception{
    query = query.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    PrintingResultsListener resultListener = new PrintingResultsListener(client.getConfig(), Format.TSV, VectorUtil.DEFAULT_COLUMN_WIDTH);
    client.runQuery(type, query, resultListener);
    return resultListener.await();
  }

  protected void testWithListener(QueryType type, String query, UserResultsListener resultListener) {
    query = query.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    client.runQuery(type, query, resultListener);
  }

  protected void testNoResult(String query, Object... args) throws Exception {
    testNoResult(1, query, args);
  }

  protected void testNoResult(int interation, String query, Object... args) throws Exception {
    query = String.format(query, args);
    logger.debug("Running query:\n--------------\n"+query);
    for (int i = 0; i < interation; i++) {
      List<QueryResultBatch> results = client.runQuery(QueryType.SQL, query);
      for (QueryResultBatch queryResultBatch : results) {
        queryResultBatch.release();
      }
    }
  }

  protected void test(String query) throws Exception{
    String[] queries = query.split(";");
    for (String q : queries) {
      if (q.trim().isEmpty()) {
        continue;
      }
      testRunAndPrint(QueryType.SQL, q);
    }
  }

  protected int testLogical(String query) throws Exception{
    return testRunAndPrint(QueryType.LOGICAL, query);
  }

  protected int testPhysical(String query) throws Exception{
    return testRunAndPrint(QueryType.PHYSICAL, query);
  }

  protected int testSql(String query) throws Exception{
    return testRunAndPrint(QueryType.SQL, query);
  }

  protected void testPhysicalFromFile(String file) throws Exception{
    testPhysical(getFile(file));
  }

  protected List<QueryResultBatch> testPhysicalFromFileWithResults(String file) throws Exception {
    return testRunAndReturn(QueryType.PHYSICAL, getFile(file));
  }

  protected void testLogicalFromFile(String file) throws Exception{
    testLogical(getFile(file));
  }

  protected void testSqlFromFile(String file) throws Exception{
    test(getFile(file));
  }

  protected String getFile(String resource) throws IOException{
    URL url = Resources.getResource(resource);
    if (url == null) {
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }

  private static class SilentListener implements UserResultsListener {
    private volatile Exception exception;
    private AtomicInteger count = new AtomicInteger();
    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void submissionFailed(RpcException ex) {
      exception = ex;
      System.out.println("Query failed: " + ex.getMessage());
      latch.countDown();
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      int rows = result.getHeader().getRowCount();
      if (result.getData() != null) {
        count.addAndGet(rows);
      }
      result.release();
      if (result.getHeader().getIsLastChunk()) {
        System.out.println("Query completed successfully with row count: " + count.get());
        latch.countDown();
      }
    }

    @Override
    public void queryIdArrived(QueryId queryId) {}

    public int waitForCompletion() throws Exception {
      latch.await();
      if (exception != null) {
        throw exception;
      }
      return count.get();
    }
  }

  protected void setColumnWidth(int columnWidth) {
    this.columnWidths = new int[] { columnWidth };
  }

  protected void setColumnWidths(int[] columnWidths) {
    this.columnWidths = columnWidths;
  }

  protected int printResult(List<QueryResultBatch> results) throws SchemaChangeException {
    int rowCount = 0;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(QueryResultBatch result : results) {
      rowCount += result.getHeader().getRowCount();
      loader.load(result.getHeader().getDef(), result.getData());
      if (loader.getRecordCount() <= 0) {
        break;
      }
      VectorUtil.showVectorAccessibleContent(loader, columnWidths);
      loader.clear();
      result.release();
    }
    System.out.println("Total record count: " + rowCount);
    return rowCount;
  }

  protected String getResultString(List<QueryResultBatch> results, String delimiter) throws SchemaChangeException {
    StringBuilder formattedResults = new StringBuilder();
    boolean includeHeader = true;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    for(QueryResultBatch result : results) {
      loader.load(result.getHeader().getDef(), result.getData());
      if (loader.getRecordCount() <= 0) {
        break;
      }
      VectorUtil.appendVectorAccessibleContent(loader, formattedResults, delimiter, includeHeader);
      if (!includeHeader) {
        includeHeader = false;
      }
      loader.clear();
      result.release();
    }

    return formattedResults.toString();
  }

  public void compareHyperVectors(Map<String, HyperVectorValueIterator> expectedRecords,
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
      for (ValueVector vv : hvi.hyperVector.getValueVectors()) {
        vv.clear();
      }
    }
    for (HyperVectorValueIterator hvi : actualRecords.values()) {
      for (ValueVector vv : hvi.hyperVector.getValueVectors()) {
        vv.clear();
      }
    }
  }

  public void compareMergedVectors(Map<String, List> expectedRecords, Map<String, List> actualRecords) throws Exception {
    for (String s : expectedRecords.keySet()) {
      assertEquals(expectedRecords.get(s).size(), actualRecords.get(s).size());
      List expectedValues = expectedRecords.get(s);
      List actualValues = actualRecords.get(s);
      for (int i = 0; i < expectedValues.size(); i++) {
        compareValues(expectedValues.get(i), actualValues.get(i), i, s);
      }
    }
  }

  public Map<String, HyperVectorValueIterator> addToHyperVectorMap(List<QueryResultBatch> records, RecordBatchLoader loader,
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
          combinedVectors.get(field).hyperVector.addVector(w.getValueVector());
        }

      }
    }
    for (HyperVectorValueIterator hvi : combinedVectors.values()) {
      hvi.determineTotalSize();
    }
    return combinedVectors;
  }

  public Map<String, List> addToCombinedVectorResults(List<QueryResultBatch> records, RecordBatchLoader loader,
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
          if (totalRecords - loader.getRecordCount() + j > 5000000) {
            continue;
          }
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

  public static class HyperVectorValueIterator implements Iterator<Object> {
    private MaterializedField mf;
    HyperVectorWrapper hyperVector;
    private int indexInVectorList;
    private int indexInCurrentVector;
    private ValueVector currVec;
    private long totalValues;
    private long totalValuesRead;
    // limit how many values will be read out of this iterator
    private long recordLimit;

    public HyperVectorValueIterator(MaterializedField mf, HyperVectorWrapper hyperVector) {
      this.mf = mf;
      this.hyperVector = hyperVector;
      this.totalValues = 0;
      this.indexInCurrentVector = 0;
      this.indexInVectorList = 0;
      this.recordLimit = -1;
    }

    public void setRecordLimit(long limit) {
      this.recordLimit = limit;
    }

    public long getTotalRecords() {
      if (recordLimit > 0) {
        return recordLimit;
      } else {
        return totalValues;
      }
    }

    public void determineTotalSize() {
      for (ValueVector vv : hyperVector.getValueVectors()) {
        this.totalValues += vv.getAccessor().getValueCount();
      }
    }

    @Override
    public boolean hasNext() {
      if (totalValuesRead == recordLimit) {
        return false;
      }
      if (indexInVectorList < hyperVector.getValueVectors().length) {
        return true;
      } else if ( indexInCurrentVector < currVec.getAccessor().getValueCount()) {
        return true;
      }
      return false;
    }

    @Override
    public Object next() {
      if (currVec == null || indexInCurrentVector == currVec.getValueCapacity()) {
        currVec = hyperVector.getValueVectors()[indexInVectorList];
        indexInVectorList++;
        indexInCurrentVector = 0;
      }
      Object obj = currVec.getAccessor().getObject(indexInCurrentVector);
      indexInCurrentVector++;
      totalValuesRead++;
      return obj;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  public void addToMaterializedResults(List<Map> materializedRecords,  List<QueryResultBatch> records, RecordBatchLoader loader,
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

  public void compareValues(Object expected, Object actual, int counter, String column) throws Exception {

    if (expected == null) {
      if (actual == null) {
        if (VERBOSE_DEBUG) {
          logger.debug("(1) at position " + counter + " column '" + column + "' matched value:  " + expected );
        }
        return;
      } else {
        throw new Exception("at position " + counter + " column '" + column + "' mismatched values, expected: " + expected + " but received " + actual);
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
      throw new Exception("at position " + counter + " column '" + column + "' mismatched values, expected: " + expected + " but received " + actual);
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
  public void compareResults(List<Map> expectedRecords, List<Map> actualRecords) throws Exception {

    BatchSchema schema = null;
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    assertEquals("Different number of records returned", expectedRecords.size(), actualRecords.size());

    String missing = "";
    int i = 0;
    int counter = 0;
    int missmatch;
    for (Map<String, Object> record : expectedRecords) {
      missmatch = 0;
      counter++;
      for (String column : record.keySet()) {
        if (  actualRecords.get(i).get(column) == null && expectedRecords.get(i).get(column) == null ) {
          continue;
        }
        if (actualRecords.get(i).get(column) == null)
          continue;
        if ( (actualRecords.get(i).get(column) == null && record.get(column) == null) || ! actualRecords.get(i).get(column).equals(record.get(column))) {
          missmatch++;
        }
      }
      if ( ! actualRecords.remove(record)) {
        missing += missmatch + ",";
      }
      else {
        i--;
      }
      i++;
    }
    logger.debug(missing);
    System.out.println(missing);
    assertEquals(0, actualRecords.size());
  }

}
