package org.apache.drill.client.jdbc;

import java.sql.SQLException;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.Handler;
import net.hydromatic.avatica.Handler.ResultSink;

import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.sql.client.full.FileSystemSchema;

public class DrillHandler implements Handler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillHandler.class);
  
  @Override
  public void onConnectionInit(AvaticaConnection connection) throws SQLException {
    DrillConnection connect = connection.unwrap(DrillConnection.class);
    try {
      connect.connect();
    } catch (RpcException ex) {
      throw new SQLException("Failure while trying to connect to server.", ex);
    }

  }

  @Override
  public void onConnectionClose(AvaticaConnection connection) throws RuntimeException {
    try {
      connection.unwrap(DrillConnection.class).closeDrillClient();
    } catch (Exception e) {
      logger.info("Failure while trying to close Drill Client.", e);
    }
  }

  @Override
  public void onStatementExecute(AvaticaStatement statement, ResultSink resultSink) throws RuntimeException {
  }

  @Override
  public void onStatementClose(AvaticaStatement statement) throws RuntimeException {
  }

}