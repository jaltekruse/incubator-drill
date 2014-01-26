package org.apache.drill.sjdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.rpc.RpcException;

public class DrillConnection implements Connection{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConnection.class);

  private final DrillClient client;
  private volatile boolean connected = false;
  private volatile String catalog;
  
  public DrillConnection() throws RpcException{
    this.client = new DrillClient();
    this.client.connect();
  }
  
  
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DrillStatement createStatement() throws SQLException {
    return new DrillStatement(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return sql;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public void commit() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollback() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws SQLException {
    client.close();
    connected = false;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return !connected;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return null;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return true;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    this.catalog = catalog;
  }

  @Override
  public String getCatalog() throws SQLException {
    return catalog;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Clob createClob() throws SQLException {
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return null;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return false;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return null;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
  }

  @Override
  public String getSchema() throws SQLException {
    return null;
  }

  @Override
  public void abort(Executor executor) throws SQLException {
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
  }
}
