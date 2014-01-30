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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaResultSet;
import net.hydromatic.avatica.AvaticaStatement;
import net.hydromatic.avatica.Cursor;
import net.hydromatic.avatica.Meta;

import org.apache.commons.lang.StringEscapeUtils;
import org.eigenbase.util.Pair;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

/**
 * Helper for implementing the {@code getXxx} methods such as
 * {@link net.hydromatic.avatica.AvaticaDatabaseMetaData#getTables}.
 */
public class MetaImpl implements Meta {
  private static final Map<Class, Pair<Integer, String>> MAP =
      ImmutableMap.<Class, Pair<Integer, String>>builder()
          .put(boolean.class, Pair.of(Types.BOOLEAN, "BOOLEAN"))
          .put(Boolean.class, Pair.of(Types.BOOLEAN, "BOOLEAN"))
          .put(byte.class, Pair.of(Types.TINYINT, "TINYINT"))
          .put(Byte.class, Pair.of(Types.TINYINT, "TINYINT"))
          .put(short.class, Pair.of(Types.SMALLINT, "SMALLINT"))
          .put(Short.class, Pair.of(Types.SMALLINT, "SMALLINT"))
          .put(int.class, Pair.of(Types.INTEGER, "INTEGER"))
          .put(Integer.class, Pair.of(Types.INTEGER, "INTEGER"))
          .put(long.class, Pair.of(Types.BIGINT, "BIGINT"))
          .put(Long.class, Pair.of(Types.BIGINT, "BIGINT"))
          .put(float.class, Pair.of(Types.FLOAT, "FLOAT"))
          .put(Float.class, Pair.of(Types.FLOAT, "FLOAT"))
          .put(double.class, Pair.of(Types.DOUBLE, "DOUBLE"))
          .put(Double.class, Pair.of(Types.DOUBLE, "DOUBLE"))
          .put(String.class, Pair.of(Types.VARCHAR, "VARCHAR"))
          .put(java.sql.Date.class, Pair.of(Types.DATE, "DATE"))
          .put(Time.class, Pair.of(Types.TIME, "TIME"))
          .put(Timestamp.class, Pair.of(Types.TIMESTAMP, "TIMESTAMP"))
          .build();

  
  private Connection connection;
  
  @Override
  public String getSqlKeywords() {
    return null;
  }

  @Override
  public String getNumericFunctions() {
    return null;
  }

  @Override
  public String getStringFunctions() {
    return null;
  }

  @Override
  public String getSystemFunctions() {
    return null;
  }

  @Override
  public String getTimeDateFunctions() {
    return null;
  }

  @Override
  public ResultSet getTables(String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList) {
    
    return null;
  }

  @Override
  public ResultSet getColumns(String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
    return null;
  }

  private String e(String s){
    return StringEscapeUtils.escapeSql(s);
  }
  
  private String p(String pattern, String name){
    if(pattern == null || Strings.isNullOrEmpty(pattern)) return name + " = " + name; 
    String escaped = StringEscapeUtils.escapeSql(pattern);
    if(escaped.indexOf('%') != -1 || escaped.indexOf('_') != -1 ){
      return name + " LIKE " + escaped;
    }else{
      return name + " = " + escaped;
    }
    
  }
  
  @Override
  public ResultSet getSchemas(String catalog, Pat schemaPattern) {
    String sql = String.format("select TABLE_SCHEM, TABLE_CATALOG from INFORMATION_SCHEMA.SCHEMAS where %s and %s order by TABLE_CATALOG, TABLE_SCHEM", p(catalog, "TABLE_CATALOG"), p(schemaPattern.s, "TABLE_SCHEM") );
    return getData(sql);
  }
  
  /**
   * Internal method to run a query for metadata purposes.
   * @param sql
   * @return
   */
  private ResultSet getData(String sql){
    return null;
  }

  @Override
  public ResultSet getCatalogs() {
    
    return null;
  }

  @Override
  public ResultSet getTableTypes() {
    return null;
  }

  @Override
  public ResultSet getProcedures(String catalog, Pat schemaPattern, Pat procedureNamePattern) {
    return null;
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, Pat schemaPattern, Pat procedureNamePattern,
      Pat columnNamePattern) {
    return null;
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, Pat columnNamePattern) {
    return null;
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, Pat schemaPattern, Pat tableNamePattern) {
    return null;
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
    return null;
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) {
    return null;
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
    return null;
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) {
    return null;
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) {
    return null;
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable) {
    return null;
  }

  @Override
  public ResultSet getTypeInfo() {
    return null;
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
    return null;
  }

  @Override
  public ResultSet getUDTs(String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types) {
    return null;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, Pat schemaPattern, Pat typeNamePattern) {
    return null;
  }

  @Override
  public ResultSet getSuperTables(String catalog, Pat schemaPattern, Pat tableNamePattern) {
    return null;
  }

  @Override
  public ResultSet getAttributes(String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern) {
    return null;
  }

  @Override
  public ResultSet getClientInfoProperties() {
    return null;
  }

  @Override
  public ResultSet getFunctions(String catalog, Pat schemaPattern, Pat functionNamePattern) {
    return null;
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern) {
    return null;
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
    return null;
  }

  @Override
  public Cursor createCursor(AvaticaResultSet resultSet) {
    return null;
  }

  @Override
  public AvaticaPrepareResult prepare(AvaticaStatement statement, String sql) {
    return null;
  }

  
}

// End MetaImpl.java
