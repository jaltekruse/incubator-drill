package org.apache.drill.client.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaDatabaseMetaData;
import net.hydromatic.avatica.AvaticaFactory;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaPreparedStatement;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.UnregisteredDriver;

/**
 * Extension of {@link net.hydromatic.avatica.AvaticaFactory}
 * for Optiq.
 */
public abstract class DrillFactory implements AvaticaFactory {
  protected final int major;
  protected final int minor;

  /** Creates a JDBC factory with given major/minor version number. */
  protected DrillFactory(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  public int getJdbcMajorVersion() {
    return major;
  }

  public int getJdbcMinorVersion() {
    return minor;
  }

  @Override
  public abstract AvaticaConnection newConnection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info);

  @Override
  public abstract AvaticaStatement newStatement(AvaticaConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability); 

  @Override
  public abstract AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection,
      AvaticaPrepareResult prepareResult, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException;

  
  @Override
  public abstract AvaticaResultSet newResultSet(AvaticaStatement statement, AvaticaPrepareResult prepareResult, TimeZone timeZone);

  @Override
  public abstract ResultSetMetaData newResultSetMetaData(AvaticaStatement statement, List<ColumnMetaData> columnMetaDataList);


  @Override
  public abstract AvaticaDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection);
  
  
}
