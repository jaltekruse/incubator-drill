/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.store.jdbc;

import org.apache.commons.lang.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.RecordReader;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class MySqlRecordReader implements RecordReader {
  private Connection connect = null;
  private Statement statement = null;
  private PreparedStatement preparedStatement = null;
  private ResultSet resultSet = null;

  private String url;
  private String schema;
  private String table;
  private List<String> columns;

  public static void main(String[] args) {
    MySqlRecordReader reader = new MySqlRecordReader();
    try {
      reader.readDataBase();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void readDataBase() throws Exception {
    try {
      // this will load the MySQL driver, each DB has its own driver
      Class.forName("com.mysql.jdbc.Driver");
      // setup the connection with the DB.
      connect = DriverManager
          .getConnection("jdbc:mysql://localhost/?"
              + "user=root&password=root");

      // statements allow to issue SQL queries to the database
      statement = connect.createStatement();
      // resultSet gets the result of the SQL query
      resultSet = statement
          .executeQuery("select * from mbed.animals");
      writeResultSet(resultSet);
    } catch (Exception ex) {
        ex.printStackTrace();
    } finally {
      close();
    }
  }

  private void writeMetaData(ResultSet resultSet) throws SQLException {
    // now get some metadata from the database
    System.out.println("The columns in the table are: ");
    System.out.println("Table: " + resultSet.getMetaData().getTableName(1));
    resultSet.getMetaData().getColumnType(0);
    for  (int i = 1; i<= resultSet.getMetaData().getColumnCount(); i++){
      System.out.println("Column " + i + " " + resultSet.getMetaData().getColumnName(i));
    }
  }

  private void writeResultSet(ResultSet resultSet) throws SQLException {
    // resultSet is initialised before the first data set
    for (int i = 1; i < resultSet.getMetaData().getColumnCount() + 1; i++ ) {
      System.out.print(StringUtils.center(resultSet.getMetaData().getColumnName(i), 20) + "|");
    }
    System.out.println();
    while (resultSet.next()) {
      for (int i = 1; i < resultSet.getMetaData().getColumnCount(); i++ ) {
        System.out.print(StringUtils.center(resultSet.getString(i) + "", 20) + "|");
      }
      System.out.println();
    }
  }

  // you need to close all three to make sure
  private void close() throws SQLException {
    resultSet.close();
    statement.close();
    connect.close();
  }
  private void close(Closeable c) {
    try {
      if (c != null) {
        c.close();
      }
    } catch (Exception e) {
      // don't throw now as it might leave following closables in undefined state
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
  }

  @Override
  public int next() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void cleanup() {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
