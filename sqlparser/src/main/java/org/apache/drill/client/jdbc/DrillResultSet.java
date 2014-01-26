
package org.apache.drill.client.jdbc;

import java.util.TimeZone;

import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaResultSetMetaData;
import net.hydromatic.avatica.AvaticaStatement;

import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

public class DrillResultSet extends AvaticaResultSet implements UserResultsListener {
  DrillResultSet(
      AvaticaStatement statement,
      DrillPrepareResult pareResult,
      AvaticaResultSetMetaData resultSetMetaData,
      TimeZone timeZone) {
    super(statement, resultSetMetaData, timeZone);
  }

  private volatile RpcException ex;


  @Override
  public void queryIdArrived(QueryId queryId) {
  }

  @Override
  public void submissionFailed(RpcException ex) {
  }

  @Override
  public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
  }

  
}
