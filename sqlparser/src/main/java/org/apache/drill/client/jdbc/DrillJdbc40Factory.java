package org.apache.drill.client.jdbc;

import net.hydromatic.optiq.jdbc.OptiqJdbc41Factory;

/**
 * Implementation of {@link net.hydromatic.avatica.AvaticaFactory}
 * for Drill and JDBC 4.0 (corresponds to JDK 1.6).
 */
public class DrillJdbc40Factory extends OptiqJdbc41Factory {
  /** Creates a factory for JDBC version 4.1. */
  public DrillJdbc40Factory() {
    super(4, 0);
  }
}
