package org.apache.drill.sjdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.proto.SchemaDefProtos.FieldDef;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;

public class DrillResultMetadata implements ResultSetMetaData{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillResultMetadata.class);

  private final RecordBatchLoader loader;
  
  public DrillResultMetadata(RecordBatchLoader loader) {
    super();
    this.loader = loader;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return loader.getSchema().getFieldCount();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    if(loader.getSchema().getColumn(column).getDataMode() == DataMode.OPTIONAL){
      return ResultSetMetaData.columnNullable;
    }else{
      return ResultSetMetaData.columnNoNulls;
    }
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return loader.getSchema().getColumn(column).getName();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return loader.getSchema().getColumn(column).getName();
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    FieldDef def = col(column).getDef();
    if(def.getMajorType().hasPrecision()) return def.getMajorType().getPrecision();
    throw new SQLException(String.format("Tried to read precision on a column that does not provide precision.  Column index %d.", column));
  }

  private MaterializedField col(int column) throws SQLException{
    MaterializedField f = loader.getSchema().getColumn(column);
    if(f == null) throw new SQLException(String.format("Attempted to access a column at index %d.  There was no column available at that index.", column));
    return f;
  }
  
  @Override
  public int getScale(int column) throws SQLException {
    FieldDef def = col(column).getDef();
    if(def.getMajorType().hasScale()) return def.getMajorType().getScale();
    throw new SQLException(String.format("Tried to read scale on a column that does not provide precision.  Column index %d.", column));
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return "";
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return "";
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return DrillToSqlTypes.getSqlType(col(column).getType());
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return col(column).getType().toString();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }
  
  
}
