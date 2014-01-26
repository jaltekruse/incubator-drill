package org.apache.drill.sjdbc;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.BigDecimalAccessor;
import org.apache.drill.exec.vector.accessor.BooleanAccessor;
import org.apache.drill.exec.vector.accessor.ByteAccessor;
import org.apache.drill.exec.vector.accessor.BytesAccessor;
import org.apache.drill.exec.vector.accessor.DateAccessor;
import org.apache.drill.exec.vector.accessor.DoubleAccessor;
import org.apache.drill.exec.vector.accessor.FloatAccessor;
import org.apache.drill.exec.vector.accessor.IntAccessor;
import org.apache.drill.exec.vector.accessor.LongAccessor;
import org.apache.drill.exec.vector.accessor.ShortAccessor;
import org.apache.drill.exec.vector.accessor.StreamAccessor;
import org.apache.drill.exec.vector.accessor.StringAccessor;
import org.apache.drill.exec.vector.accessor.TimeAccessor;
import org.apache.drill.exec.vector.accessor.TimestampAccessor;

import com.google.common.base.Charsets;

public abstract class AbstractDrillResultSet implements ResultSet{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractDrillResultSet.class);
  
  private RecordBatchLoader currentBatch;
  private int currentColumn;
  int currentRecord;
  
  private ValueVector v(int columnIndex) throws SQLException {
    VectorWrapper<?> wrapper = currentBatch.getValueAccessorById(columnIndex, null);
    if(wrapper == null) throw new SQLException(String.format("You requested an unknown column index.", columnIndex));
    this.currentColumn = columnIndex;
    return (ValueVector) wrapper.getValueVector();
  }
  
  private ValueVector.Accessor va(int columnIndex) throws SQLException {
    return v(columnIndex).getAccessor();
  }
  
  @SuppressWarnings("unchecked")
  private <T extends ValueVector> T v(int columnIndex, Class<T> clazz, String typeName) throws SQLException{
    ValueVector v = v(columnIndex);
    if(clazz.isAssignableFrom(v.getClass())){
      return (T) v;
    }else{
      throw new SQLException(String.format("You attempted to read the a value of type %s from the columnIndex %d.  This failed as the column is of type %s.", typeName, columnIndex, v.getField().getDef().getMajorType()));
    }
  }
  
  @SuppressWarnings("unchecked")
  private <T extends ValueVector.Accessor> T va(int columnIndex, Class<T> clazz) throws SQLException{
    ValueVector.Accessor a = va(columnIndex);
    if(clazz.isAssignableFrom(a.getClass())){
      return (T) a;
    }else{
      throw new SQLException(String.format("You attempted to read the a value of type %s from the columnIndex %d.  This failed as the column is of type %s.", clazz.getName().replace("Accessor", ""), columnIndex, v(columnIndex).getField().getDef().getMajorType()));
    }
  }
  
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public boolean wasNull() throws SQLException {
    return va(currentColumn).isNull(currentRecord);
  }

  
  private int findColumn0(String columnLabel) throws SQLException {
    int i =0;
    for(MaterializedField field : currentBatch.getSchema()){
      if(field.getName().equals(columnLabel)) return i;
      i++;
    }
    throw new SQLException(String.format("Unknown column with name %s.", columnLabel));
  }
  
  @Override
  public String getString(int columnIndex) throws SQLException {
    return va(columnIndex, StringAccessor.class).getStringValue(currentRecord);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return va(columnIndex, BooleanAccessor.class).getBooleanValue(currentRecord);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return va(columnIndex, ByteAccessor.class).getByteValue(currentRecord);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return va(columnIndex, ShortAccessor.class).getShortValue(currentRecord);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return va(columnIndex, IntAccessor.class).getIntValue(currentRecord);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return va(columnIndex, LongAccessor.class).getLongValue(currentRecord);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return va(columnIndex, FloatAccessor.class).getFloatValue(currentRecord);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return va(columnIndex, DoubleAccessor.class).getDoubleValue(currentRecord);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return va(columnIndex, BytesAccessor.class).getBytesValue(currentRecord);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return va(columnIndex, DateAccessor.class).getDateValue(currentRecord);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return va(columnIndex, TimeAccessor.class).getTimeValue(currentRecord);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return va(columnIndex, TimestampAccessor.class).getTimestampValue(currentRecord);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return va(columnIndex, StreamAccessor.class).getStreamValue(currentRecord);
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return va(columnIndex, StreamAccessor.class).getStreamValue(currentRecord);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return va(columnIndex, StreamAccessor.class).getStreamValue(currentRecord);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn0(columnLabel));
  }


  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn0(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return getByte(findColumn0(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn0(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn0(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn0(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn0(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn0(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn0(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn0(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn0(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn0(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return getAsciiStream(findColumn0(columnLabel));
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return getUnicodeStream(findColumn0(columnLabel));
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return getBinaryStream(findColumn0(columnLabel));
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
  }

  @Override
  public String getCursorName() throws SQLException {
    return null;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new DrillResultMetadata(currentBatch);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return va(columnIndex).getObject(currentRecord);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn0(columnLabel));
  }


  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    MaterializedField f = currentBatch.getSchema().getColumn(columnIndex);
    switch(f.getType().getMinorType()){
    case VAR16CHAR:
    case FIXED16CHAR:
      return new InputStreamReader(getBinaryStream(columnIndex), Charsets.UTF_16);
    case VARCHAR:
    case FIXEDCHAR:
      return new InputStreamReader(getBinaryStream(columnIndex), Charsets.UTF_8);
    }
    
    throw new SQLException("Attempted to retrieve a value as a character stream where that values type does not support content stream reading.");
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return getCharacterStream(findColumn0(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return va(columnIndex, BigDecimalAccessor.class).getBigDecimalValue(currentRecord); 
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn0(columnLabel));
  }


  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLException("Repositioning cursors is not supported by the Drill JDBC driver.");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw new SQLException("Repositioning cursors is not supported by the Drill JDBC driver.");
  }

  @Override
  public boolean previous() throws SQLException {
    throw new SQLException("Repositioning cursors is not supported by the Drill JDBC driver.");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLException("Repositioning cursors is not supported by the Drill JDBC driver.");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // ignored.
  }

  @Override
  public int getFetchSize() throws SQLException {
    return 0;
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return null;
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
  }

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return null;
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return null;
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return null;
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return null;
  }
  
}
