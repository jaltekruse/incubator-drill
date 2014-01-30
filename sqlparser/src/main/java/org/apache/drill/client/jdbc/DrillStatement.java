package org.apache.drill.client.jdbc;

import java.sql.SQLException;

import net.hydromatic.avatica.AvaticaStatement;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.QueryResult;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

public abstract class DrillStatement extends AvaticaStatement{
  DrillStatement(DrillConnectionImpl connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  private volatile QueryId queryId;
  private volatile RpcException ex;
  
  @Override
  public DrillConnectionImpl getConnection() {
    return (DrillConnectionImpl) connection;
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    DrillClient client = getConnection().getDrillClient();
    client.runQuery(QueryType.SQL, sql, this);
    return true;
  }

  @Override
  public void queryIdArrived(QueryId queryId) {
    this.queryId = queryId;
  }

  @Override
  public void submissionFailed(RpcException ex) {
  }

  @Override
  public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
    QueryResult header = result.getHeader();
    if(header) 
  }

  
  
}