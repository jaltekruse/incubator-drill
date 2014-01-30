package org.apache.drill.sjdbc;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;

public class DrillToSqlTypes{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillToSqlTypes.class);
  
  public static int getSqlType(MajorType type) throws SQLException{
    if(type.getMode() == DataMode.REPEATED) return Types.ARRAY;
    
    switch(type.getMinorType()){
    case BIGINT:
      return Types.BIGINT;
    case BIT:
      return Types.BIT;
    case DATE:
      return Types.DATE;
    case DATETIME:
      return Types.TIMESTAMP;
    case DECIMAL12:
      return Types.DECIMAL;
    case DECIMAL16:
      return Types.DECIMAL;
    case DECIMAL4:
      return Types.DECIMAL;
    case DECIMAL8:
      return Types.DECIMAL;
    case FIXED16CHAR:
      return Types.NCHAR;
    case FIXEDBINARY:
      return Types.BINARY;
    case FIXEDCHAR:
      return Types.NCHAR;
    case FLOAT4:
      return Types.FLOAT;
    case FLOAT8:
      return Types.DOUBLE;
    case INT:
      return Types.INTEGER;
    case MAP:
      return Types.STRUCT;
    case MONEY:
      return Types.DECIMAL;
    case SMALLINT:
      return Types.SMALLINT;
    case TIME:
      return Types.TIME;
    case TIMESTAMP:
      return Types.TIMESTAMP;
    case TIMETZ:
      return Types.TIMESTAMP;
    case TINYINT:
      return Types.TINYINT;
    case UINT1:
      return Types.TINYINT;
    case UINT2:
      return Types.SMALLINT;
    case UINT4:
      return Types.INTEGER;
    case UINT8:
      return Types.BIGINT;
    case VAR16CHAR:
      return Types.NVARCHAR;
    case VARBINARY:
      return Types.VARBINARY;
    case VARCHAR:
      return Types.NVARCHAR;
      
    case INTERVAL:
    case LATE:
    case NULL:
    case REPEATMAP:
    default:
      throw new SQLFeatureNotSupportedException("This Drill type isn't currently supported by the JDBC driver.");
    }
  }
}
