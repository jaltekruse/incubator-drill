package org.apache.drill.client.jdbc;

import net.hydromatic.avatica.DriverVersion;
import net.hydromatic.avatica.Handler;
import net.hydromatic.avatica.HandlerImpl;
import net.hydromatic.avatica.UnregisteredDriver;

/**
 * Drill JDBC driver.
 */
public class Driver extends UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:drill:";

  static {
    new Driver().register();
  }

  public Driver() {
    super();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
    case JDBC_30:
      throw new UnsupportedOperationException();
    case JDBC_40:
      return "org.apache.drill.client.jdbc.DrillJdbc40Factory";
    case JDBC_41:
    default:
      return "org.apache.drill.client.jdbc.DrillJdbc41Factory";
    }
  }

  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(Driver.class, "org.apache.drill.jdbc.properties", "Drill Driver", "unknown version",
        "Drill", "unknown version");
  }

  @Override
  protected Handler createHandler() {
    return new HandlerImpl();
  }
}
