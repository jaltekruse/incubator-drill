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
package org.apache.drill.exec.server;

import static org.apache.drill.exec.ExecConstants.SLICE_TARGET;
import static org.apache.drill.exec.ExecConstants.SLICE_TARGET_DEFAULT;
import static org.apache.drill.exec.planner.physical.PlannerSettings.HASHAGG;
import static org.apache.drill.exec.planner.physical.PlannerSettings.PARTITION_SENDER_SET_THREADS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.Pair;
import org.apache.drill.QueryTestUtil;
import org.apache.drill.SingleRowListener;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.concurrent.ExtendedLatch;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ZookeeperHelper;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.physical.impl.ScreenCreator;
import org.apache.drill.exec.physical.impl.WriterRecordBatch;
import org.apache.drill.exec.physical.impl.mergereceiver.MergingRecordBatch;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderRootExec;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionerDecorator;
import org.apache.drill.exec.physical.impl.unorderedreceiver.UnorderedReceiverBatch;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.ExceptionWrapper;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.store.pojo.PojoRecordReader;
import org.apache.drill.exec.testing.ControlsInjectionUtil;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.util.TestUtilities;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.ForemanException;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * Test how resilient drillbits are to throwing exceptions during various phases of query
 * execution by injecting exceptions at various points and to cancellations in various phases.
 * The test cases are mentioned in DRILL-2383.
 */
public class TestDrillbitResilience {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(TestDrillbitResilience.class);

  private static ZookeeperHelper zkHelper;
  private static RemoteServiceSet remoteServiceSet;
  private static final Map<String, Drillbit> drillbits = new HashMap<>();
  private static DrillClient drillClient;

  /**
   * Note: Counting sys.memory executes a fragment on every drillbit. This is a better check in comparison to
   * counting sys.drillbits.
   */
  private static final String TEST_QUERY = "select * from sys.memory";

  private static void startDrillbit(final String name, final RemoteServiceSet remoteServiceSet) {
    if (drillbits.containsKey(name)) {
      throw new IllegalStateException("Drillbit named \"" + name + "\" already exists");
    }

    try {
      @SuppressWarnings("resource")
      final Drillbit drillbit = Drillbit.start(zkHelper.getConfig(), remoteServiceSet);
      drillbits.put(name, drillbit);
    } catch (final DrillbitStartupException e) {
      throw new RuntimeException("Failed to start Drillbit \"" + name + "\"", e);
    }
  }

  /**
   * Shutdown the specified drillbit.
   *
   * @param name name of the drillbit
   */
  private static void stopDrillbit(final String name) {
    @SuppressWarnings("resource")
    final Drillbit drillbit = drillbits.get(name);
    if (drillbit == null) {
      throw new IllegalStateException("No Drillbit named \"" + name + "\" found");
    }

    try {
      drillbit.close();
    } catch (final Exception e) {
      final String message = "Error shutting down Drillbit \"" + name + "\"";
      System.err.println(message + '.');
      logger.warn(message, e);
    }
  }

  /**
   * Shutdown all the drillbits.
   */
  private static void stopAllDrillbits() {
    for (String name : drillbits.keySet()) {
      stopDrillbit(name);
    }
    drillbits.clear();
  }

  /*
   * Canned drillbit names.
   */
  private final static String DRILLBIT_ALPHA = "alpha";
  private final static String DRILLBIT_BETA = "beta";
  private final static String DRILLBIT_GAMMA = "gamma";

  @BeforeClass
  public static void startSomeDrillbits() throws Exception {
    // turn off the HTTP server to avoid port conflicts between the drill bits
    System.setProperty(ExecConstants.HTTP_ENABLE, "false");

    zkHelper = new ZookeeperHelper();
    zkHelper.startZookeeper(1);

    // use a non-null service set so that the drillbits can use port hunting
    remoteServiceSet = RemoteServiceSet.getLocalServiceSet();

    // create name-addressable drillbits
    startDrillbit(DRILLBIT_ALPHA, remoteServiceSet);
    startDrillbit(DRILLBIT_BETA, remoteServiceSet);
    startDrillbit(DRILLBIT_GAMMA, remoteServiceSet);

    // set up a unique temporary folder for this test run
    String dfsTestTmpSchemaLocation = TestUtilities.createTempDir();
    for (Drillbit bit : drillbits.values()) {
      TestUtilities.setupDefaultTestStoragePluginConfig(bit, dfsTestTmpSchemaLocation);
    }

    // create a client
    final DrillConfig drillConfig = zkHelper.getConfig();
    drillClient = QueryTestUtil.createClient(drillConfig, remoteServiceSet, 1, null);
    clearAllInjections();
  }

  @AfterClass
  public static void shutdownAllDrillbits() {
    if (drillClient != null) {
      drillClient.close();
      drillClient = null;
    }

    stopAllDrillbits();

    if (remoteServiceSet != null) {
      AutoCloseables.close(remoteServiceSet, logger);
      remoteServiceSet = null;
    }

    zkHelper.stopZookeeper();
  }

  /**
   * Clear all injections.
   */
  private static void clearAllInjections() {
    Preconditions.checkNotNull(drillClient);
    ControlsInjectionUtil.clearControls(drillClient);
  }

  /**
   * Check that all the drillbits are ok.
   * <p/>
   * <p>The current implementation does this by counting the number of drillbits using a query.
   */
  private static void assertDrillbitsOk() {
      final SingleRowListener listener = new SingleRowListener() {
          private final BufferAllocator bufferAllocator = new TopLevelAllocator(zkHelper.getConfig());
          private final RecordBatchLoader loader = new RecordBatchLoader(bufferAllocator);

          @Override
          public void rowArrived(final QueryDataBatch queryResultBatch) {
            // load the single record
            final QueryData queryData = queryResultBatch.getHeader();
            try {
              loader.load(queryData.getDef(), queryResultBatch.getData());
              // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
              // SchemaChangeException, so check/clean catch clause below.
            } catch (final SchemaChangeException e) {
              fail(e.toString());
            }
            assertEquals(1, loader.getRecordCount());

            // there should only be one column
            final BatchSchema batchSchema = loader.getSchema();
            assertEquals(1, batchSchema.getFieldCount());

            // the column should be an integer
            final MaterializedField countField = batchSchema.getColumn(0);
            final MinorType fieldType = countField.getType().getMinorType();
            assertEquals(MinorType.BIGINT, fieldType);

            // get the column value
            final VectorWrapper<?> vw = loader.iterator().next();
            final Object obj = vw.getValueVector().getAccessor().getObject(0);
            assertTrue(obj instanceof Long);
            final Long countValue = (Long) obj;

            // assume this means all the drillbits are still ok
            assertEquals(drillbits.size(), countValue.intValue());

            loader.clear();
          }

          @Override
          public void cleanup() {
            bufferAllocator.close();
          }
        };

    try {
      QueryTestUtil.testWithListener(drillClient, QueryType.SQL, "select count(*) from sys.memory", listener);
      listener.waitForCompletion();
      final QueryState state = listener.getQueryState();
      assertTrue(String.format("QueryState should be COMPLETED (and not %s).", state), state == QueryState.COMPLETED);
    } catch (final Exception e) {
      throw new RuntimeException("Couldn't query active drillbits", e);
    }

    final List<DrillPBError> errorList = listener.getErrorList();
    assertTrue("There should not be any errors when checking if Drillbits are OK.", errorList.isEmpty());
  }

  @SuppressWarnings("static-method")
  @After
  public void checkDrillbits() {
    clearAllInjections(); // so that the drillbit check itself doesn't trigger anything
    assertDrillbitsOk(); // TODO we need a way to do this without using a query
  }

  /**
   * Set the given controls.
   */
  private static void setControls(final String controls) {
    ControlsInjectionUtil.setControls(drillClient, controls);
  }

  /**
   * Create a single exception injection.
   *
   * @param siteClass      the injection site class
   * @param desc           the injection site description
   * @param exceptionClass the class of the exception to throw
   * @return the created controls JSON as string
   */
  private static String createSingleException(final Class<?> siteClass, final String desc,
                                              final Class<? extends Throwable> exceptionClass) {
    final String siteClassName = siteClass.getName();
    final String exceptionClassName = exceptionClass.getName();
    return "{\"injections\":[{"
      + "\"type\":\"exception\","
      + "\"siteClass\":\"" + siteClassName + "\","
      + "\"desc\":\"" + desc + "\","
      + "\"nSkip\":0,"
      + "\"nFire\":1,"
      + "\"exceptionClass\":\"" + exceptionClassName + "\""
      + "}]}";
  }

  /**
   * Create a single exception injection.
   *
   * @param siteClass      the injection site class
   * @param desc           the injection site description
   * @param exceptionClass the class of the exception to throw
   * @return the created controls JSON as string
   */
  private static String createSingleUncheckedException(final Class<?> siteClass, final String desc) {
    final String siteClassName = siteClass.getName();
//    final String exceptionClassName = exceptionClass.getName();
    return "{\"injections\":[{"
        + "\"type\":\"exception\","
        + "\"siteClass\":\"" + siteClassName + "\","
        + "\"desc\":\"" + desc + "\","
        + "\"nSkip\":0,"
        + "\"nFire\":1"
//        + "\"exceptionClass\":\"" + exceptionClassName + "\""
        + "}]}";
  }

  /**
   * Create a single exception injection.
   *
   * @param siteClass      the injection site class
   * @param desc           the injection site description
   * @param exceptionClass the class of the exception to throw
   * @param bitName        the drillbit name which should be injected into
   * @return the created controls JSON as string
   */
  private static String createSingleExceptionOnBit(final Class<?> siteClass, final String desc,
                                                   final Class<? extends Throwable> exceptionClass,
                                                   final String bitName) {
    final String siteClassName = siteClass.getName();
    final String exceptionClassName = exceptionClass.getName();
    @SuppressWarnings("resource")
    final Drillbit drillbit = drillbits.get(bitName);
    if (drillbit == null) {
      throw new IllegalStateException("No Drillbit named \"" + bitName + "\" found");
    }

    final DrillbitEndpoint endpoint = drillbit.getContext().getEndpoint();
    return "{\"injections\":[{"
      + "\"address\":\"" + endpoint.getAddress() + "\","
      + "\"port\":\"" + endpoint.getUserPort() + "\","
      + "\"type\":\"exception\","
      + "\"siteClass\":\"" + siteClassName + "\","
      + "\"desc\":\"" + desc + "\","
      + "\"nSkip\":0,"
      + "\"nFire\":1,"
      + "\"exceptionClass\":\"" + exceptionClassName + "\""
      + "}]}";
  }

  /**
   * Check that the injected exception is what we were expecting.
   *
   * @param throwable      the throwable that was caught (by the test)
   * @param exceptionClass the expected exception class
   * @param desc           the expected exception site description
   */
  private static void assertExceptionInjected(final Throwable throwable,
                                              final Class<? extends Throwable> exceptionClass, final String desc) {
    assertTrue("Throwable was not of UserException type.", throwable instanceof UserException);
    final ExceptionWrapper cause = ((UserException) throwable).getOrCreatePBError(false).getException();
    assertEquals("Exception class names should match.", exceptionClass.getName(), cause.getExceptionClass());
    assertEquals("Exception sites should match.", desc, cause.getMessage());
  }

  @Test
  public void settingNoopInjectionsAndQuery() {
    final String controls = createSingleExceptionOnBit(getClass(), "noop", RuntimeException.class, DRILLBIT_BETA);
    setControls(controls);
    try {
      QueryTestUtil.test(drillClient, TEST_QUERY);
    } catch (final Exception e) {
      fail(e.getMessage());
    }
  }

  /**
   * Test throwing exceptions from sites within the Foreman class, as specified by the site
   * description
   *
   * @param desc site description
   */
  private static void testForeman(final String desc) {
    final String controls = createSingleException(Foreman.class, desc, ForemanException.class);
    setControls(controls);
    try {
      QueryTestUtil.test(drillClient, TEST_QUERY);
      fail();
    } catch (final Exception e) {
      assertExceptionInjected(e, ForemanException.class, desc);
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void foreman_runTryBeginning() {
    testForeman("run-try-beginning");
  }

  @SuppressWarnings("static-method")
  @Test
  public void foreman_runTryEnd() {
    testForeman("run-try-end");
  }

  /**
   * Tests can use this listener to wait, until the submitted query completes or fails, by
   * calling #waitForCompletion.
   */
  private static class WaitUntilCompleteListener implements UserResultsListener {
    private final ExtendedLatch latch = new ExtendedLatch(1); // to signal completion
    protected QueryId queryId = null;
    protected volatile Pointer<Exception> ex = new Pointer<>();
    protected volatile QueryState state = null;

    /**
     * Method that sets the exception if the condition is not met.
     */
    protected final void check(final boolean condition, final String format, final Object... args) {
      if (!condition) {
        ex.value = new IllegalStateException(String.format(format, args));
      }
    }

    /**
     * Method that cancels and resumes the query, in order.
     */
    protected final void cancelAndResume() {
      Preconditions.checkNotNull(queryId);
      final ExtendedLatch trigger = new ExtendedLatch(1);
      (new CancellingThread(queryId, ex, trigger)).start();
      (new ResumingThread(queryId, ex, trigger)).start();
    }

    @Override
    public void queryIdArrived(final QueryId queryId) {
      this.queryId = queryId;
    }

    @Override
    public void submissionFailed(final UserException ex) {
      this.ex.value = ex;
      state = QueryState.FAILED;
      latch.countDown();
    }

    @Override
    public void queryCompleted(final QueryState state) {
      this.state = state;
      latch.countDown();
    }

    @Override
    public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
      result.release();
    }

    public final Pair<QueryState, Exception> waitForCompletion() {
      latch.awaitUninterruptibly();
      return new Pair<>(state, ex.value);
    }
  }

  private static class ListenerThatCancelsQueryAfterFirstBatchOfData extends WaitUntilCompleteListener {
    private boolean cancelRequested = false;

    @Override
    public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
      if (!cancelRequested) {
        check(queryId != null, "Query id should not be null, since we have waited long enough.");
        (new CancellingThread(queryId, ex, null)).start();
        cancelRequested = true;
      }
      result.release();
    }
  };

  /**
   * Thread that cancels the given query id. After the cancel is acknowledged, the latch is counted down.
   */
  private static class CancellingThread extends Thread {
    private final QueryId queryId;
    private final Pointer<Exception> ex;
    private final ExtendedLatch latch;

    public CancellingThread(final QueryId queryId, final Pointer<Exception> ex, final ExtendedLatch latch) {
      this.queryId = queryId;
      this.ex = ex;
      this.latch = latch;
    }

    @Override
    public void run() {
      final DrillRpcFuture<Ack> cancelAck = drillClient.cancelQuery(queryId);
      try {
        cancelAck.checkedGet();
      } catch (final RpcException ex) {
        this.ex.value = ex;
      }
      if (latch != null) {
        latch.countDown();
      }
    }
  }

  /**
   * Thread that resumes the given query id. After the latch is counted down, the resume signal is sent, until then
   * the thread waits without interruption.
   */
  private static class ResumingThread extends Thread {
    private final QueryId queryId;
    private final Pointer<Exception> ex;
    private final ExtendedLatch latch;

    public ResumingThread(final QueryId queryId, final Pointer<Exception> ex, final ExtendedLatch latch) {
      this.queryId = queryId;
      this.ex = ex;
      this.latch = latch;
    }

    @Override
    public void run() {
      latch.awaitUninterruptibly();
      final DrillRpcFuture<Ack> resumeAck = drillClient.resumeQuery(queryId);
      try {
        resumeAck.checkedGet();
      } catch (final RpcException ex) {
        this.ex.value = ex;
      }
    }
  }

  /**
   * Given the result of {@link WaitUntilCompleteListener#waitForCompletion}, this method fails if the state is not
   * as expected or if an exception is thrown.
   */
  private static void assertCompleteState(final Pair<QueryState, Exception> result, final QueryState expectedState) {
    final QueryState actualState = result.getFirst();
    final Exception exception = result.getSecond();
    if (actualState != expectedState || exception != null) {
      fail(String.format("Query state is incorrect (expected: %s, actual: %s) AND/OR \nException thrown: %s",
        expectedState, actualState, exception == null ? "none." : exception));
    }
  }

  /**
   * Given a set of controls, this method ensures that the TEST_QUERY completes with a CANCELED state.
   */
  private static void assertCancelledWithoutException(final String controls, final WaitUntilCompleteListener listener) {
    assertCancelled(controls, TEST_QUERY, listener);
  }

  /**
   * Given a set of controls, this method ensures that the given query completes with a CANCELED state.
   */
  private static void assertCancelled(final String controls, final String testQuery,
      final WaitUntilCompleteListener listener) {
    setControls(controls);

    QueryTestUtil.testWithListener(drillClient, QueryType.SQL, testQuery, listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    assertCompleteState(result, QueryState.CANCELED);
  }

  private static void setSessionOption(final String option, final String value) {
    try {
      final List<QueryDataBatch> results = drillClient.runQuery(QueryType.SQL,
          String.format("alter session set `%s` = %s", option, value));
      for (final QueryDataBatch data : results) {
        data.release();
      }
    } catch(RpcException e) {
      fail(String.format("Failed to set session option `%s` = %s, Error: %s", option, value, e.toString()));
    }
  }

  private static String createPauseInjection(final Class siteClass, final String siteDesc, final int nSkip) {
    return "{\"injections\" : [{"
      + "\"type\" : \"pause\"," +
      "\"siteClass\" : \"" + siteClass.getName() + "\","
      + "\"desc\" : \"" + siteDesc + "\","
      + "\"nSkip\" : " + nSkip
      + "}]}";
  }

  private static String createPauseInjection(final Class siteClass, final String siteDesc) {
    return createPauseInjection(siteClass, siteDesc, 0);
  }

  @Test // To test pause and resume. Test hangs if resume did not happen.
  public void passThrough() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener() {
      @Override
      public void queryIdArrived(final QueryId queryId) {
        super.queryIdArrived(queryId);
        final ExtendedLatch trigger = new ExtendedLatch(1);
        (new ResumingThread(queryId, ex, trigger)).start();
        trigger.countDown();
      }
    };

    final String controls = createPauseInjection(PojoRecordReader.class, "read-next");
    setControls(controls);

    QueryTestUtil.testWithListener(drillClient, QueryType.SQL, TEST_QUERY, listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    assertCompleteState(result, QueryState.COMPLETED);
  }

  @Test // Cancellation TC 1: cancel before any result set is returned
  public void cancelBeforeAnyResultsArrive() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener() {

      @Override
      public void queryIdArrived(final QueryId queryId) {
        super.queryIdArrived(queryId);
        cancelAndResume();
      }
    };

    final String controls = createPauseInjection(Foreman.class, "foreman-ready");
    assertCancelledWithoutException(controls, listener);
  }

  @Test // Cancellation TC 2: cancel in the middle of fetching result set
  public void cancelInMiddleOfFetchingResults() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener() {
      private boolean cancelRequested = false;

      @Override
      public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
        if (!cancelRequested) {
          check(queryId != null, "Query id should not be null, since we have waited long enough.");
          cancelAndResume();
          cancelRequested = true;
        }
        result.release();
      }
    };

    // skip once i.e. wait for one batch, so that #dataArrived above triggers #cancelAndResume
    final String controls = createPauseInjection(ScreenCreator.class, "sending-data", 1);
    assertCancelledWithoutException(controls, listener);
  }


  @Test // Cancellation TC 3: cancel after all result set are produced but not all are fetched
  public void cancelAfterAllResultsProduced() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener() {
      private int count = 0;

      @Override
      public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
        if (++count == drillbits.size()) {
          check(queryId != null, "Query id should not be null, since we have waited long enough.");
          cancelAndResume();
        }
        result.release();
      }
    };

    final String controls = createPauseInjection(ScreenCreator.class, "send-complete");
    assertCancelledWithoutException(controls, listener);
  }

  @Test // Cancellation TC 4: cancel after everything is completed and fetched
  public void cancelAfterEverythingIsCompleted() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener() {
      private int count = 0;

      @Override
      public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
        if (++count == drillbits.size()) {
          check(queryId != null, "Query id should not be null, since we have waited long enough.");
          cancelAndResume();
        }
        result.release();
      }
    };

    final String controls = createPauseInjection(Foreman.class, "foreman-cleanup");
    assertCancelledWithoutException(controls, listener);
  }

  @Test // Completion TC 1: success
  public void successfullyCompletes() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener();
    QueryTestUtil.testWithListener(drillClient, QueryType.SQL, TEST_QUERY, listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    assertCompleteState(result, QueryState.COMPLETED);
  }

  /**
   * Given a set of controls, this method ensures TEST_QUERY fails with the given class and desc.
   */
  private static void assertFailsWithException(final String controls, final Class<? extends Throwable> exceptionClass,
                                               final String exceptionDesc) {
    assertFailsWithException(TEST_QUERY, controls, exceptionClass, exceptionDesc);
  }

  /**
   * Given a set of controls, this method ensures the provided query fails with the given class and desc.
   */
  private static void assertFailsWithException(final String testQuery, final String controls, final Class<? extends Throwable> exceptionClass,
                                               final String exceptionDesc) {
    setControls(controls);
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener();
    QueryTestUtil.testWithListener(drillClient, QueryType.SQL, testQuery, listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    final QueryState state = result.getFirst();
    assertTrue(String.format("Query state should be FAILED (and not %s).", state), state == QueryState.FAILED);
    assertExceptionInjected(result.getSecond(), exceptionClass, exceptionDesc);
  }

  @Test // Completion TC 2: failed query - before query is executed - while sql parsing
  public void failsWhenParsing() {
    final String exceptionDesc = "sql-parsing";
    final Class<? extends Throwable> exceptionClass = ForemanSetupException.class;
    final String controls = createSingleException(DrillSqlWorker.class, exceptionDesc, exceptionClass);
    assertFailsWithException(controls, exceptionClass, exceptionDesc);
  }

  @Test // Completion TC 3: failed query - before query is executed - while sending fragments to other drillbits
  public void failsWhenSendingFragments() {
    final String exceptionDesc = "send-fragments";
    final Class<? extends Throwable> exceptionClass = ForemanException.class;
    final String controls = createSingleException(Foreman.class, exceptionDesc, exceptionClass);
    assertFailsWithException(controls, exceptionClass, exceptionDesc);
  }

  @Test // Completion TC 4: failed query - during query execution
  public void failsDuringExecution() {
    final String exceptionDesc = "fragment-execution";
    final Class<? extends Throwable> exceptionClass = IOException.class;
    final String controls = createSingleException(FragmentExecutor.class, exceptionDesc, exceptionClass);
    assertFailsWithException(controls, exceptionClass, exceptionDesc);
  }

  @Test
  public void ioExceptionWhileWritingTable() throws Exception {
    final String[] injectionSiteNames = { WriterRecordBatch.IO_EXCEPTION_INNER_NEXT_INJECT_SITE,
        WriterRecordBatch.UNCHECKED_EXCEPTION_INNER_NEXT_INJECT_SITE,
        WriterRecordBatch.UNCHECKED_EXCEPTION_SETUP_NEW_SCHEMA_SITE,
    };

    final Class<? extends Throwable>[] exceptionClasses = new Class[]{
        IOException.class,
        RuntimeException.class,
        RuntimeException.class,
    };

    assertEquals(injectionSiteNames.length, exceptionClasses.length);
    int numTablesCreated = 0;
    // test each of the injection sites
    for (int injectionSiteIndex = 0; injectionSiteIndex < injectionSiteNames.length; injectionSiteIndex++) {
      final String controls =
          createSingleException(
              WriterRecordBatch.class,
              injectionSiteNames[injectionSiteIndex],
              exceptionClasses[injectionSiteIndex]);
      // TODO - add complex types to parquet and json writing
      final String queryFormatStr = "create table small_test_file_%d as select * from cp.`tpch/lineitem.parquet`";
      QueryTestUtil.test(drillClient, "use dfs_test.tmp");
      String[] formats = new String[]{"json", "csv", "parquet"};
      // test each of the output formats
      for (int formatIndex = 0; formatIndex < formats.length; formatIndex++) {
        final long before = countAllocatedMemory();
        QueryTestUtil.test(drillClient,
            String.format("alter session set `%s` = '%s'",
                ExecConstants.OUTPUT_FORMAT_VALIDATOR.getOptionName(),
                formats[formatIndex]));
        assertFailsWithException(
            String.format(queryFormatStr, numTablesCreated),
            controls, exceptionClasses[injectionSiteIndex], injectionSiteNames[injectionSiteIndex]);
        numTablesCreated++;
        final long after = countAllocatedMemory();
        assertEquals(String.format("We are leaking %d bytes", after - before), before, after);
      }
    }
  }

  private long countAllocatedMemory() {
    // wait to make sure all fragments finished cleaning up
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // just ignore
    }

    long allocated = 0;
    for (String name : drillbits.keySet()) {
      allocated += drillbits.get(name).getContext().getAllocator().getAllocatedMemory();
    }

    return allocated;
  }

  /**
   * Test cancelling query interrupts currently blocked FragmentExecutor threads waiting for some event to happen.
   * Specifically tests cancelling fragment which has {@link MergingRecordBatch} blocked waiting for data.
   */
  @Test
  public void testInterruptingBlockedMergingRecordBatch() {
    final String control = createPauseInjection(MergingRecordBatch.class, "waiting-for-data", 1);
    testInterruptingBlockedFragmentsWaitingForData(control);
  }

  /**
   * Test cancelling query interrupts currently blocked FragmentExecutor threads waiting for some event to happen.
   * Specifically tests cancelling fragment which has {@link UnorderedReceiverBatch} blocked waiting for data.
   */
  @Test
  public void testInterruptingBlockedUnorderedReceiverBatch() {
    final String control = createPauseInjection(UnorderedReceiverBatch.class, "waiting-for-data", 1);
    testInterruptingBlockedFragmentsWaitingForData(control);
  }

  private static void testInterruptingBlockedFragmentsWaitingForData(final String control) {
    try {
      setSessionOption(SLICE_TARGET, "1");
      setSessionOption(HASHAGG.getOptionName(), "false");

      final String query = "SELECT sales_city, COUNT(*) cnt FROM cp.`region.json` GROUP BY sales_city";
      assertCancelled(control, query, new ListenerThatCancelsQueryAfterFirstBatchOfData());
    } finally {
      setSessionOption(SLICE_TARGET, Long.toString(SLICE_TARGET_DEFAULT));
      setSessionOption(HASHAGG.getOptionName(), HASHAGG.getDefault().bool_val.toString());
    }
  }

  /**
   * Tests interrupting the fragment thread that is running {@link PartitionSenderRootExec}.
   * {@link PartitionSenderRootExec} spawns threads for partitioner. Interrupting fragment thread should also interrupt
   * the partitioner threads.
   */
  @Test
  public void testInterruptingPartitionerThreadFragment() {
    try {
      setSessionOption(SLICE_TARGET, "1");
      setSessionOption(HASHAGG.getOptionName(), "true");
      setSessionOption(PARTITION_SENDER_SET_THREADS.getOptionName(), "6");

      final String controls = "{\"injections\" : ["
          + "{"
          + "\"type\" : \"latch\","
          + "\"siteClass\" : \"" + PartitionerDecorator.class.getName() + "\","
          + "\"desc\" : \"partitioner-sender-latch\""
          + "},"
          + "{"
          + "\"type\" : \"pause\","
          + "\"siteClass\" : \"" + PartitionerDecorator.class.getName() + "\","
          + "\"desc\" : \"wait-for-fragment-interrupt\","
          + "\"nSkip\" : 1"
          + "}" +
          "]}";

      final String query = "SELECT sales_city, COUNT(*) cnt FROM cp.`region.json` GROUP BY sales_city";
      assertCancelled(controls, query, new ListenerThatCancelsQueryAfterFirstBatchOfData());
    } finally {
      setSessionOption(SLICE_TARGET, Long.toString(SLICE_TARGET_DEFAULT));
      setSessionOption(HASHAGG.getOptionName(), HASHAGG.getDefault().bool_val.toString());
      setSessionOption(PARTITION_SENDER_SET_THREADS.getOptionName(),
          Long.toString(PARTITION_SENDER_SET_THREADS.getDefault().num_val));
    }
  }
}
