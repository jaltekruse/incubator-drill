/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
 */
package org.apache.drill.client.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaFactory;
import net.hydromatic.avatica.Helper;
import net.hydromatic.avatica.Meta;
import net.hydromatic.avatica.UnregisteredDriver;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.rpc.RpcException;

/**
 * Implementation of JDBC connection in the Optiq engine.
 * 
 * <p>
 * Abstract to allow newer versions of JDBC to add methods.
 * </p>
 */
abstract class DrillConnectionImpl extends AvaticaConnection implements DrillConnection {

  // must be package-protected
  static final Trojan TROJAN = createTrojan();

  private final DrillClient client;
  
  protected DrillConnectionImpl(Driver driver, AvaticaFactory factory, String url, Properties info) {
    super(driver, factory, url, info);
    this.client = new DrillClient();
  }

  
  @Override
  protected Meta createMeta() {
    return new MetaImpl(this);
  }

  MetaImpl meta() {
    return (MetaImpl) meta;
  }

  public ConnectionConfig config() {
    return connectionConfig(info);
  }
  
  DrillClient getDrillClient(){
    return client;
  }

  @Override
  public void connect() throws RpcException {
    client.connect();
  }

  @Override
  public void closeDrillClient() {
    client.close();
  }
  
  
  public static ConnectionConfig connectionConfig(final Properties properties) {
    return new ConnectionConfig() {
    };
  }

  @Override
  public DrillStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    DrillStatement statement = (DrillStatement) super.createStatement(resultSetType, resultSetConcurrency,
        resultSetHoldability);
    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw Helper.INSTANCE.createException("Error while preparing statement [" + //
        sql + //
        "].  ", //
        new UnsupportedOperationException("The Drill JDBC driver doesn't currently support prepared statements."));
  }

  public Properties getProperties() {
    return info;
  }

  @Override
  public TimeZone getTimeZone() {
    throw new UnsupportedOperationException();
  }

  // do not make public
  UnregisteredDriver getDriver() {
    return driver;
  }

}
