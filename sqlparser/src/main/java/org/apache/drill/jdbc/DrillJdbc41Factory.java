package org.apache.drill.jdbc;



import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaDatabaseMetaData;
import net.hydromatic.avatica.AvaticaFactory;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaPreparedStatement;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaResultSetMetaData;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.UnregisteredDriver;

class DrillJdbc41Factory implements AvaticaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillJdbc41Factory.class);

 private final int major;
 private final int minor;

 /** Creates a JDBC factory. */
 public DrillJdbc41Factory() {
   this(4, 1);
 }

 /** Creates a JDBC factory with given major/minor version number. */
 protected DrillJdbc41Factory(int major, int minor) {
   this.major = major;
   this.minor = minor;
 }

 public int getJdbcMajorVersion() {
   return major;
 }

 public int getJdbcMinorVersion() {
   return minor;
 }

 public AvaticaConnection newConnection(
     UnregisteredDriver driver,
     AvaticaFactory factory,
     String url,
     Properties info) {
   return new DrillJdbc41Connection(driver, factory, url, info);
 }

 public AvaticaDatabaseMetaData newDatabaseMetaData(
     AvaticaConnection connection) {
   return new DrillJdbc41DatabaseMetaData(connection);
 }

 public AvaticaStatement newStatement(
     AvaticaConnection connection,
     int resultSetType,
     int resultSetConcurrency,
     int resultSetHoldability) {
   return new DrillJdbc41Statement(
       connection, resultSetType, resultSetConcurrency,
       resultSetHoldability);
 }

 public AvaticaPreparedStatement newPreparedStatement(
     AvaticaConnection connection,
     AvaticaPrepareResult prepareResult,
     int resultSetType,
     int resultSetConcurrency,
     int resultSetHoldability) throws SQLException {
   return new DrillJdbc41PreparedStatement(
       connection, prepareResult, resultSetType, resultSetConcurrency,
       resultSetHoldability);
 }

 public AvaticaResultSet newResultSet(
     AvaticaStatement statement,
     AvaticaPrepareResult prepareResult,
     TimeZone timeZone) {
   final ResultSetMetaData metaData =
       newResultSetMetaData(statement, prepareResult.getColumnList());
   return new AvaticaResultSet(
       statement, prepareResult, metaData, timeZone);
 }

 public AvaticaResultSetMetaData newResultSetMetaData(
     AvaticaStatement statement,
     List<ColumnMetaData> columnMetaDataList) {
   return new AvaticaResultSetMetaData(
       statement, null, columnMetaDataList);
 }

 private static class DrillJdbc41Connection extends AvaticaConnection {
   DrillJdbc41Connection(UnregisteredDriver driver,
       AvaticaFactory factory,
       String url,
       Properties info) {
     super(driver, factory, url, info);
   }
 }

 private static class DrillJdbc41Statement extends AvaticaStatement {
   public DrillJdbc41Statement(AvaticaConnection connection,
       int resultSetType,
       int resultSetConcurrency,
       int resultSetHoldability) {
     super(
         connection, resultSetType, resultSetConcurrency,
         resultSetHoldability);
   }

   public void closeOnCompletion() throws SQLException {
     this.closeOnCompletion = true;
   }

   public boolean isCloseOnCompletion() throws SQLException {
     return closeOnCompletion;
   }
 }

 private static class DrillJdbc41PreparedStatement
     extends AvaticaPreparedStatement {
   DrillJdbc41PreparedStatement(
       AvaticaConnection connection,
       AvaticaPrepareResult sql,
       int resultSetType,
       int resultSetConcurrency,
       int resultSetHoldability) throws SQLException {
     super(
         connection, sql, resultSetType, resultSetConcurrency,
         resultSetHoldability);
   }

   public void setRowId(
       int parameterIndex,
       RowId x) throws SQLException {
     getParameter(parameterIndex).setRowId(x);
   }

   public void setNString(
       int parameterIndex, String value) throws SQLException {
     getParameter(parameterIndex).setNString(value);
   }

   public void setNCharacterStream(
       int parameterIndex,
       Reader value,
       long length) throws SQLException {
     getParameter(parameterIndex).setNCharacterStream(value, length);
   }

   public void setNClob(
       int parameterIndex,
       NClob value) throws SQLException {
     getParameter(parameterIndex).setNClob(value);
   }

   public void setClob(
       int parameterIndex,
       Reader reader,
       long length) throws SQLException {
     getParameter(parameterIndex).setClob(reader, length);
   }

   public void setBlob(
       int parameterIndex,
       InputStream inputStream,
       long length) throws SQLException {
     getParameter(parameterIndex).setBlob(inputStream, length);
   }

   public void setNClob(
       int parameterIndex,
       Reader reader,
       long length) throws SQLException {
     getParameter(parameterIndex).setNClob(reader, length);
   }

   public void setSQLXML(
       int parameterIndex, SQLXML xmlObject) throws SQLException {
     getParameter(parameterIndex).setSQLXML(xmlObject);
   }

   public void setAsciiStream(
       int parameterIndex,
       InputStream x,
       long length) throws SQLException {
     getParameter(parameterIndex).setAsciiStream(x, length);
   }

   public void setBinaryStream(
       int parameterIndex,
       InputStream x,
       long length) throws SQLException {
     getParameter(parameterIndex).setBinaryStream(x, length);
   }

   public void setCharacterStream(
       int parameterIndex,
       Reader reader,
       long length) throws SQLException {
     getParameter(parameterIndex).setCharacterStream(reader, length);
   }

   public void setAsciiStream(
       int parameterIndex, InputStream x) throws SQLException {
     getParameter(parameterIndex).setAsciiStream(x);
   }

   public void setBinaryStream(
       int parameterIndex, InputStream x) throws SQLException {
     getParameter(parameterIndex).setBinaryStream(x);
   }

   public void setCharacterStream(
       int parameterIndex, Reader reader) throws SQLException {
     getParameter(parameterIndex).setCharacterStream(reader);
   }

   public void setNCharacterStream(
       int parameterIndex, Reader value) throws SQLException {
     getParameter(parameterIndex).setNCharacterStream(value);
   }

   public void setClob(
       int parameterIndex,
       Reader reader) throws SQLException {
     getParameter(parameterIndex).setClob(reader);
   }

   public void setBlob(
       int parameterIndex, InputStream inputStream) throws SQLException {
     getParameter(parameterIndex).setBlob(inputStream);
   }

   public void setNClob(
       int parameterIndex, Reader reader) throws SQLException {
     getParameter(parameterIndex).setNClob(reader);
   }

   public void closeOnCompletion() throws SQLException {
     closeOnCompletion = true;
   }

   public boolean isCloseOnCompletion() throws SQLException {
     return closeOnCompletion;
   }
 }

 private static class DrillJdbc41DatabaseMetaData
     extends AvaticaDatabaseMetaData {
   DrillJdbc41DatabaseMetaData(AvaticaConnection connection) {
     super(connection);
   }
 }
}

//End AvaticaJdbc41Factory.java

