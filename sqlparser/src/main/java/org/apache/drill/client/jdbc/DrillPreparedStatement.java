package org.apache.drill.client.jdbc;

import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaPreparedStatement;

import java.sql.*;

abstract class DrillPreparedStatement extends AvaticaPreparedStatement {

  protected DrillPreparedStatement(DrillConnectionImpl connection, AvaticaPrepareResult prepareResult,
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    super(connection, prepareResult, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public DrillConnectionImpl getConnection() {
    return (DrillConnectionImpl) super.getConnection();
  }

}
