package org.apache.drill.exec.store.hive.schema;

import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.type.SqlTypeName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

public class DrillHiveTable extends DrillTable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillHiveTable.class);
  
  private final Table hiveTable;
  
  public DrillHiveTable(String name, String storageEngineName, Object selection, StorageEngineConfig storageEngineConfig, Table hiveTable) {
    super(name, storageEngineName, selection, storageEngineConfig);
    this.hiveTable = hiveTable;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    ArrayList<RelDataType> typeList = new ArrayList<>();
    ArrayList<String> fieldNameList = new ArrayList<>();

    ArrayList<StructField> hiveFields = hiveTable.getFields();
    for(StructField hiveField : hiveFields) {
      fieldNameList.add(hiveField.getFieldName());
      typeList.add(getRelDataTypeFromHiveType(typeFactory, hiveField.getFieldObjectInspector()));
    }

    for (FieldSchema field : hiveTable.getPartitionKeys()) {
      fieldNameList.add(field.getName());
      typeList.add(getRelDataTypeFromHiveTypeString(typeFactory, field.getType()));
    }

    final RelDataType rowType = typeFactory.createStructType(typeList, fieldNameList);
    return rowType;
  }

  private RelDataType getRelDataTypeFromHiveTypeString(RelDataTypeFactory typeFactory, String type) {
    switch(type) {
      case "boolean":
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);

      case "tinyint":
        return typeFactory.createSqlType(SqlTypeName.TINYINT);

      case "smallint":
        return typeFactory.createSqlType(SqlTypeName.SMALLINT);

      case "int":
        return typeFactory.createSqlType(SqlTypeName.INTEGER);

      case "bigint":
        return typeFactory.createSqlType(SqlTypeName.BIGINT);

      case "float":
        return typeFactory.createSqlType(SqlTypeName.FLOAT);

      case "double":
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);

      case "date":
        return typeFactory.createSqlType(SqlTypeName.DATE);

      case "timestamp":
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);

      case "binary":
        return typeFactory.createSqlType(SqlTypeName.BINARY);

      case "decimal":
        return typeFactory.createSqlType(SqlTypeName.DECIMAL);

      case "string":
      case "varchar": {
        return typeFactory.createTypeWithCharsetAndCollation(
                typeFactory.createSqlType(SqlTypeName.VARCHAR), /*input type*/
                Charset.forName("ISO-8859-1"), /*unicode char set*/
                SqlCollation.IMPLICIT /* TODO: need to decide if implicit is the correct one */
        );
      }

      default:
        throw new RuntimeException("Unknown or unsupported hive type: " + type);
    }
  }

  private RelDataType getRelDataTypeFromHivePrimitiveType(RelDataTypeFactory typeFactory, PrimitiveObjectInspector poi) {
    switch(poi.getPrimitiveCategory()) {
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case BYTE:
        return typeFactory.createSqlType(SqlTypeName.TINYINT);

      case SHORT:
        return typeFactory.createSqlType(SqlTypeName.SMALLINT);

      case INT:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);

      case LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);

      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.FLOAT);

      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);

      case DATE:
        return typeFactory.createSqlType(SqlTypeName.DATE);

      case TIMESTAMP:
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);

      case BINARY:
        return typeFactory.createSqlType(SqlTypeName.BINARY);

      case DECIMAL:
        return typeFactory.createSqlType(SqlTypeName.DECIMAL);

      case STRING:
      case VARCHAR: {
        return typeFactory.createTypeWithCharsetAndCollation(
          typeFactory.createSqlType(SqlTypeName.VARCHAR), /*input type*/
          Charset.forName("ISO-8859-1"), /*unicode char set*/
          SqlCollation.IMPLICIT /* TODO: need to decide if implicit is the correct one */
        );
      }

      case UNKNOWN:
      case VOID:
      default:
        throw new RuntimeException("Unknown or unsupported hive type");
    }
  }

  private RelDataType getRelDataTypeFromHiveType(RelDataTypeFactory typeFactory, ObjectInspector oi) {
    switch(oi.getCategory()) {
      case PRIMITIVE:
        return getRelDataTypeFromHivePrimitiveType(typeFactory, ((PrimitiveObjectInspector) oi));
      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      default:
        throw new RuntimeException("Unknown or unsupported hive type");
    }
  }
}
