/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.jdbc;

import net.hydromatic.avatica.DriverVersion;
import net.hydromatic.avatica.Handler;

import org.apache.drill.exec.client.DrillClient;

/**
 * JDBC driver for Apache Drill.
 */
public class Driver extends net.hydromatic.avatica.UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:drill:";

  private volatile DrillHandler handler;
  
  static {
    new Driver().register();
  }

  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  protected DriverVersion createDriverVersion() {
    return new DrillDriverVersion();
  }

  

  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
    case JDBC_30:
      return "net.hydromatic.avatica.AvaticaFactoryJdbc3Impl";
    case JDBC_40:
      return "net.hydromatic.avatica.AvaticaJdbc40Factory";
    case JDBC_41:
    default:
      return "net.hydromatic.avatica.AvaticaJdbc41Factory";
    }
  }
  public DrillClient getClient(){
    return handler.getClient();
  }
  
  @Override
  protected Handler createHandler() {
    this.handler = new DrillHandler(false);
    return handler;
  }
  
}

// End Driver.java
