package org.apache.drill.sjdbc;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.drill.common.config.DrillConfig;

public class DrillJdbcDriver implements java.sql.Driver {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillJdbcDriver.class);

  
  static{
    try {
      DriverManager.registerDriver(new DrillJdbcDriver());
    } catch (SQLException e) {
      System.err.println("Failure while attempting to register Drill JDBC driver.");
      e.printStackTrace(System.err);
    }
  }
  
  final DrillConfig config = DrillConfig.create();
  
  @Override
  public DrillConnection connect(String url, Properties info) throws SQLException {
    String zookeeper = info.getProperty("zk");
    return new DrillConnection(config, zookeeper);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith("jdbc:drill:");
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return null;
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }
}
