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
package org.apache.drill.exec.rpc.user;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcBus;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

/**
 * Encapsulates the future management of query submissions. This entails a potential race condition. Normal ordering is:
 * 1. Submit query to be executed. 2. Receive QueryHandle for buffer management 3. Start receiving results batches for
 * query.
 *
 * However, 3 could potentially occur before 2. As such, we need to handle this case and then do a switcheroo.
 *
 */
public class QueryResultHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryResultHandler.class);

  /**
   * Per query:  Current listener for results.
   * <p>
   *   Concurrency:  Access by SubmissionLister for query-ID message vs.
   *   access by batchArrived is not otherwise synchronized.
   * </p>
   */
  private final ConcurrentMap<QueryId, UserResultsListener> queryIdToResultsListenersMap =
      Maps.newConcurrentMap();

  /**
   * Per query:  Any is-last-chunk batch being deferred until the next batch
   * (normally one with COMPLETED) arrives.
   * <ul>
   *   <li>Last-chunk batch is added (and not passed on) when it arrives.</li>
   *   <li>Last-chunk batch is removed (and passed on) when next batch arrives
   *       and has state {@link QueryState.COMPLETED}.</li>
   *   <li>Last-chunk batch is removed (and not passed on) when next batch
   *       arrives and has state {@link QueryState.CANCELED} or
   *       {@link QueryState.FAILED}.</li>
   * </ul>
   */
  private final Map<QueryId, QueryResultBatch> queryIdToDeferredLastChunkBatchesMap =
      new HashMap<>();


  public RpcOutcomeListener<QueryId> getWrappedListener(UserResultsListener resultsListener) {
    return new SubmissionListener(resultsListener);
  }

  /**
   * Maps internal low-level API protocol to listener API protocol, deferring
   * is-last-chunk batches until completion.
   * <p>
   *   xxx
   * </p>
   */
  public void batchArrived(ConnectionThrottle throttle,
                           ByteBuf pBody, ByteBuf dBody) throws RpcException {
    final QueryResult result = RpcBus.get(pBody, QueryResult.PARSER);
    // Current batch coming in.  (Not necessarily passed along now or ever.)
    final QueryResultBatch currentBatch = new QueryResultBatch(result, (DrillBuf) dBody);

    final QueryId queryId = result.getQueryId();
    final QueryState queryState = currentBatch.getHeader().getQueryState();

    logger.debug( "batchArrived: isLastChunk: {}, queryState: {}, queryId = {}",
                  currentBatch.getHeader().getIsLastChunk(), queryState, queryId );
    logger.trace( "batchArrived: currentBatch = {}", currentBatch );

    final boolean isFailureBatch = QueryState.FAILED == queryState;
    final boolean isCompletionBatchxx = QueryState.COMPLETED == queryState;
    final boolean isIsLastChunkBatchToDelayxx =
        currentBatch.getHeader().getIsLastChunk() && QueryState.PENDING == queryState;
    final boolean isTerminalBatch;
    switch ( queryState ) {
      case PENDING:
         isTerminalBatch = false;
         break;
      case FAILED:
      case CANCELED:
      case COMPLETED:
        isTerminalBatch = false;
        break;
      default:
        assert false : "Unexpected/unhandled QueryState " + queryState
                       + " (queryId: " + queryId +  ")";
        isTerminalBatch = false;
        break;
    }
    assert isFailureBatch || currentBatch.getHeader().getErrorCount() == 0
        : "Error count for the query batch is non-zero but QueryState != FAILED";

    UserResultsListener resultsListener = queryIdToResultsListenersMap.get( queryId );
    logger.trace("For QueryId [{}], retrieved results listener {}", queryId,
                 resultsListener);
    if (resultsListener == null) {
      // WHO?? didn't get query ID response and set submission listener yet,
      // so install a buffering listener for now

      BufferingResultsListener bl = new BufferingResultsListener();
      resultsListener = queryIdToResultsListenersMap.putIfAbsent( queryId, bl );
      // If we had a successful insertion, use that reference.  Otherwise, just
      // throw away the new buffering listener.
      if (resultsListener == null) {
        resultsListener = bl;
      }
      if ( queryId.toString().equals("") ) {
        failAll();
      }
    }

    try {
      if (isFailureBatch) {
        // Failure case--pass on via submissionFailed(...).

        String message = buildErrorMessage(currentBatch);
        resultsListener.submissionFailed(new RpcException(message));

        currentBatch.release();
        // Note: Listener and any delayed batch are removed in finally clause.
      } else {
        // Success (data, completion, or cancelation) case--pass on via
        // resultArrived, delaying any last-chunk batches until following
        // COMPLETED batch and omitting COMPLETED batch.

        // If is last-chunk batch, save until next batch for query (normally a
        // COMPLETED batch) comes in.

        if ( isIsLastChunkBatchToDelayxx ) {
          // We have a (non-failure) is-last-chunk batch--defer it until we get
          // the query's COMPLETED batch.

          QueryResultBatch expectNone;
          assert null == ( expectNone = queryIdToDeferredLastChunkBatchesMap.get( queryId ) )
              : "Already have pending last-batch QueryResultBatch " + expectNone
                + " (at receiving last-batch QueryResultBatch " + currentBatch + ") for query " + queryId;
          queryIdToDeferredLastChunkBatchesMap.put( queryId, currentBatch );
        } else {
          // xxx not deferring

          // Batch to send out in response to current batch.  (Current batch
          // unless delaying or sending delayed last-chunk batch.
          final QueryResultBatch batchToSend;  //???? rename? "outputBatch"?
          final boolean releaseCurrentBatchxxx;      //???? revisit

          if ( isCompletionBatchxx ) {
            // We have a COMPLETED batch--we should have saved an is-last-chunk
            // batch and we must pass that on now (that we've see COMPLETED).

            batchToSend = queryIdToDeferredLastChunkBatchesMap.get( queryId );
            assert null != batchToSend
                : "No pending last-batch QueryResultsBatch saved, at COMPLETED"
                + " QueryResultsBatch " + currentBatch + " for query " + queryId;
            releaseCurrentBatchxxx = true;
          } else {
            // We should have a PENDING non last-chunk batch or a CANCELED batch.

            batchToSend = currentBatch;
            releaseCurrentBatchxxx = false;
          }
          try {
            resultsListener.resultArrived( batchToSend, throttle );
          } catch ( Exception e ) {
            batchToSend.release();
            resultsListener.submissionFailed(new RpcException(e));
          }
          finally {
            if ( releaseCurrentBatchxxx ) {
              currentBatch.release();
            }
          }
        }
      }
    } finally {
      if ( isTerminalBatch ) {
        // Remove any queued is-last-chunk batch:
        queryIdToDeferredLastChunkBatchesMap.remove( queryId );
        // TODO:  What exactly are we checking for?  How should we really check
        // for it?
        if ( (! ( resultsListener instanceof BufferingResultsListener )
             || ((BufferingResultsListener)resultsListener).output != null ) ) {
          queryIdToResultsListenersMap.remove( queryId, resultsListener );
        }
      }
    }
  }

  protected String buildErrorMessage(QueryResultBatch batch) {
    StringBuilder sb = new StringBuilder();
    for (UserBitShared.DrillPBError error : batch.getHeader().getErrorList()) {
      sb.append(error.getMessage());
      sb.append("\n");
    }
    return sb.toString();
  }

  private void failAll() {
    for (UserResultsListener l : queryIdToResultsListenersMap.values()) {
      l.submissionFailed(new RpcException("Received result without QueryId"));
    }
  }

  private static class BufferingResultsListener implements UserResultsListener {

    private ConcurrentLinkedQueue<QueryResultBatch> results = Queues.newConcurrentLinkedQueue();
    private volatile boolean finished = false;
    private volatile RpcException ex;
    private volatile UserResultsListener output;
    private volatile ConnectionThrottle throttle;

    public boolean transferTo(UserResultsListener l) {
      synchronized (this) {
        output = l;
        boolean last = false;
        for (QueryResultBatch r : results) {
          l.resultArrived(r, throttle);
          last = r.getHeader().getIsLastChunk();
        }
        if (ex != null) {
          l.submissionFailed(ex);
          return true;
        }
        return last;
      }
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      this.throttle = throttle;
      if (result.getHeader().getIsLastChunk()) {
        finished = true;
      }

      synchronized (this) {
        if (output == null) {
          this.results.add(result);
        } else {
          output.resultArrived(result, throttle);
        }
      }
    }

    @Override
    public void submissionFailed(RpcException ex) {
      finished = true;
      synchronized (this) {
        if (output == null) {
          this.ex = ex;
        } else{
          output.submissionFailed(ex);
        }
      }
    }

    public boolean isFinished() {
      return finished;
    }

    @Override
    public void queryIdArrived(QueryId queryId) {
    }

  }

  private class SubmissionListener extends BaseRpcOutcomeListener<QueryId> {
    private UserResultsListener resultsListener;

    public SubmissionListener(UserResultsListener resultsListener) {
      super();
      this.resultsListener = resultsListener;
    }

    @Override
    public void failed(RpcException ex) {
      resultsListener.submissionFailed(ex);
    }

    @Override
    public void success(QueryId queryId, ByteBuf buf) {
      resultsListener.queryIdArrived(queryId);
      logger.debug("Received QueryId {} successfully.  Adding results listener {}.",
                   queryId, resultsListener);
      UserResultsListener oldListener =
          queryIdToResultsListenersMap.putIfAbsent(queryId, resultsListener);

      // We need to deal with the situation where we already received results by
      // the time we got the query id back.  In that case, we'll need to
      // transfer the buffering listener over, grabbing a lock against reception
      // of additional results during the transition.
      if (oldListener != null) {
        logger.debug("Unable to place user results listener, buffering listener was already in place.");
        if (oldListener instanceof BufferingResultsListener) {
          queryIdToResultsListenersMap.remove(oldListener);
          boolean all = ((BufferingResultsListener) oldListener).transferTo(this.resultsListener);
          // simply remove the buffering listener if we already have the last response.
          if (all) {
            queryIdToResultsListenersMap.remove(oldListener);
          } else {
            boolean replaced = queryIdToResultsListenersMap.replace(queryId, oldListener, resultsListener);
            if (!replaced) {
              throw new IllegalStateException(); // TODO: Say what the problem is!
            }
          }
        } else {
          throw new IllegalStateException("Trying to replace a non-buffering User Results listener.");
        }
      }

    }

  }

}
