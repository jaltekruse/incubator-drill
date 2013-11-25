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
package org.apache.drill.sql.client.full;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.DefaultExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.drill.jdbc.DrillTable;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class FileSystemSchema implements Schema{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemSchema.class);
  
  private ConcurrentMap<String, TableInSchema> tables = Maps.newConcurrentMap();

  private final JavaTypeFactory typeFactory;
  private final Schema parentSchema;
  private final String name;
  private final Expression expression = new DefaultExpression(Object.class);
  private final SchemaProvider schemaProvider;
  private final StorageEngineConfig config;
  
  public FileSystemSchema(StorageEngineConfig config, SchemaProvider schemaProvider, JavaTypeFactory typeFactory, Schema parentSchema, String name) {
    super();
    this.typeFactory = typeFactory;
    this.parentSchema = parentSchema;
    this.name = name;
    this.schemaProvider = schemaProvider;
    this.config = config;
  }

  @Override
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  @Override
  public Schema getParentSchema() {
    return parentSchema;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Expression getExpression() {
    return expression;
  }

  @Override
  public QueryProvider getQueryProvider() {    
    throw new UnsupportedOperationException();
  }


  @Override
  public Collection<TableFunctionInSchema> getTableFunctions(String name) {
    return Collections.emptyList();
  }
  
  @Override
  public Multimap<String, TableFunctionInSchema> getTableFunctions() {
    return ArrayListMultimap.create();
  }

  @Override
  public Collection<String> getSubSchemaNames() {
    return Collections.EMPTY_LIST;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public <E> Table<E> getTable(String name, Class<E> elementType) {
    if( !elementType.isAssignableFrom(DrillTable.class)) throw new UnsupportedOperationException();
    TableInfo info = (TableInfo) tables.get(name);
    if(info != null) return (Table<E>) info.table;
    Object selection = schemaProvider.getSelectionBaseOnName(name);
    if(selection == null) return null;
    
    DrillTable table = DrillTable.createTable(typeFactory, this, name, this.name, config, selection);
    info = new TableInfo(name, table);
    TableInfo oldInfo = (TableInfo) tables.putIfAbsent(name, info);
    if(oldInfo != null) return (Table<E>) oldInfo.table;
    return (Table<E>) table;
  }

  @Override
  public Map<String, TableInSchema> getTables() {
    return this.tables;
  }
  
  private class TableInfo extends TableInSchema{
    
    final DrillTable table;

    public TableInfo(String name, DrillTable table) {
      super(FileSystemSchema.this, name, TableType.TABLE);
      this.table = table;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> Table<E> getTable(Class<E> elementType) {
      if( !elementType.isAssignableFrom(DrillTable.class)) throw new UnsupportedOperationException();
      return (Table<E>) table;
    }
    
    
  }
  
}
