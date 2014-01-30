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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import net.hydromatic.avatica.AvaticaFactory;


/**
 * JDBC driver for Apache Drill.
 */
public class RefDriver extends net.hydromatic.avatica.UnregisteredDriver {
  public static final String CONNECT_STRING_PREFIX = "jdbc:drillref:";

  private volatile DrillHandler handler;
  
  static {
    new RefDriver().register();
  }

  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  protected DrillDriverVersion createDriverVersion() {
    return new DrillDriverVersion();
  }

  @Override
  protected AvaticaFactory createFactory() {
    return super.createFactory();
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return super.connect(url, info);
  }

  
//  @Override
//  protected Function0<OptiqPrepare> createPrepareFactory() {
//    return new Function0<OptiqPrepare>() {
//      @Override
//      public OptiqPrepare apply() {
//        return new DrillPrepareImpl(null);
//      }
//    };
//  }
//
//  public DrillClient getClient(){
//    return handler.getClient();
//  }
//  
//  @Override
//  protected Handler createHandler() {
//    this.handler = new DrillHandler(true);
//    return handler;
//  }
  
}

// End Driver.java
