package org.apache.drill.sjdbc;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

public class DrillResultSet extends AbstractDrillResultSet implements UserResultsListener{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillResultSet.class);
  

  private volatile RpcException ex;
  private volatile boolean completed = false;
  
  final BlockingQueue<QueryResultBatch> queue = new ArrayBlockingQueue<>(100);

  @Override
  public void submissionFailed(RpcException ex) {
    this.ex = ex;
    completed = true;
    System.out.println("Query failed: " + ex);
  }

  @Override
  public void resultArrived(QueryResultBatch result) {
    logger.debug("Result arrived {}", result);
    queue.add(result);
    if(result.getHeader().getIsLastChunk()){
      completed = true;
    }
    if (result.getHeader().getErrorCount() > 0) {
      submissionFailed(new RpcException(String.format("%s", result.getHeader().getErrorList())));
    }
  }

  public boolean completed(){
    return completed;
  }

  public QueryResultBatch getNext() throws RpcException, InterruptedException{
    while(true){
      if(ex != null) throw ex;
      if(completed && queue.isEmpty()){
        return null;
      }else{
        QueryResultBatch q = queue.poll(50, TimeUnit.MILLISECONDS);
        if(q != null) return q;
      }
      
    }
  }

  @Override
  public void queryIdArrived(QueryId queryId) {
  }
  }
