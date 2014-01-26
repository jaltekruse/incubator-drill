package org.apache.drill.sjdbc;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import com.google.common.collect.Queues;

public class DrillResultSet extends AbstractDrillResultSet {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillResultSet.class);

  private SchemaChangeListener changeListener;
  final Listener listener = new Listener();
  private final DrillStatement statement; 
  private long recordBatchCount;
  private boolean started = false;
  private boolean finished = false;
  private volatile QueryId queryId;
  private final DrillClient client;
  
  public DrillResultSet(DrillClient client, DrillStatement statement, BufferAllocator allocator) {
    super(allocator);
    this.statement = statement;
    this.client = client;
  }

  @Override
  public boolean next() throws SQLException {
    if(!started) started = true;
    if(finished) return false;
    
    if(currentRecord+1 < currentBatch.getRecordCount()){
      currentRecord++;
      return true;
    }else{
      try {
        QueryResultBatch qrb = listener.getNext();
        recordBatchCount++;
        if(qrb == null){
          finished = true;
          return false;
        }else{
          currentRecord = 0;
          boolean changed = currentBatch.load(qrb.getHeader().getDef(), qrb.getData());
          if(changed && changeListener != null) changeListener.schemaChanged(currentBatch.getSchema());
          return true;
        }
      } catch (RpcException | InterruptedException | SchemaChangeException e) {
        throw new SQLException("Failure while trying to get next result batch.", e);
      }
      
    }
  }

  
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if(iface == DrillResultSet.class) return (T) this;
    throw new SQLException(String.format("Failure converting to type %s.", iface.getName()));
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface == DrillResultSet.class;
  }

  @Override
  public void close() throws SQLException {
    if(queryId != null) client.cancelQuery(queryId);
    listener.close();
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return !started;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return finished;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return recordBatchCount == 1 && currentRecord == 0;
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new SQLFeatureNotSupportedException(); 
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement getStatement() throws SQLException {
    return statement;
  }
  
  
  private class Listener implements UserResultsListener {
    private static final int MAX = 100;
    private volatile RpcException ex;
    private volatile boolean completed = false;
    private volatile boolean autoread = true;
    private volatile ConnectionThrottle throttle;
    private volatile boolean closed = false;

    final LinkedBlockingDeque<QueryResultBatch> queue = Queues.newLinkedBlockingDeque();

    @Override
    public void submissionFailed(RpcException ex) {
      this.ex = ex;
      completed = true;
      close();
      System.out.println("Query failed: " + ex);
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      logger.debug("Result arrived {}", result);
      
      // if we're in a closed state, just release the message.
      if(closed){
        result.release();
        completed = true;
        return;
      }
      
      // we're active, let's add to the queue.
      queue.add(result);
      if(queue.size() >= MAX - 1){
        throttle.setAutoRead(false);
        this.throttle = throttle;
        autoread = false;
      }
      
      if (result.getHeader().getIsLastChunk()) {
        completed = true;
      }
      
      if (result.getHeader().getErrorCount() > 0) {
        submissionFailed(new RpcException(String.format("%s", result.getHeader().getErrorList())));
      }
    }

    public QueryResultBatch getNext() throws RpcException, InterruptedException {
      while (true) {
        if (ex != null)
          throw ex;
        if (completed && queue.isEmpty()) {
          return null;
        } else {
          QueryResultBatch q = queue.poll(50, TimeUnit.MILLISECONDS);
          if (q != null){
            if(!autoread && queue.size() < MAX/2){
              autoread = true;
              throttle.setAutoRead(true);
              throttle = null;
            }
            return q;
          }
            
        }

      }
    }

    void close(){
      closed = true;
      while(!queue.isEmpty()){
        queue.poll().getData().release();
      }
    }
    
    @Override
    public void queryIdArrived(QueryId queryId) {
      DrillResultSet.this.queryId = queryId;
    }
  }



}
