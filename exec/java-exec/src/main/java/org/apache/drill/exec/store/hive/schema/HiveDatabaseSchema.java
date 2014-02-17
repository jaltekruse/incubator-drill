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
package org.apache.drill.exec.store.hive.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.hydromatic.linq4j.expressions.DefaultExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.TableFunction;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveStorageEngine;
import org.apache.drill.exec.store.hive.HiveStorageEngine.HiveSchemaProvider;
import org.apache.drill.exec.store.hive.HiveStorageEngineConfig;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class HiveDatabaseSchema implements Schema{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveDatabaseSchema.class);

  private final Expression expression = new DefaultExpression(Object.class);
  private final SchemaPlus parentSchema;
  private final HiveSchema hiveSchema;
  private final String name;
  private final HiveSchemaProvider schemaProvider;
  private final HiveStorageEngineConfig config;

  public HiveDatabaseSchema(//
      HiveStorageEngineConfig config, //
      HiveSchema hiveSchema,
      HiveStorageEngine.HiveSchemaProvider schemaProvider, //
      SchemaPlus parentSchema, //
      String name) {
    super();
    this.parentSchema = parentSchema;
    this.hiveSchema = hiveSchema;
    this.name = name;
    this.schemaProvider = schemaProvider;
    this.config = config;
  }

  @Override
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public SchemaPlus getParentSchema() {
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
  
  // TODO: Need to integrates UDFs?
  @Override
  public Collection<TableFunction> getTableFunctions(String name) {
    return Collections.emptyList();
  }

  /**
   * No more sub schemas within a database schema
   */
  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }

  @Override
  public DrillTable getTable(String name) {
    Object selection = schemaProvider.getSelectionBaseOnName(String.format("%s.%s",this.name, name));
    if(selection == null) return null;
    org.apache.hadoop.hive.metastore.api.Table t = ((HiveReadEntry) selection).getTable();
    if (t == null) {
      logger.debug("Table name {} is invalid", name);
      return null;
    }
    org.apache.hadoop.hive.ql.metadata.Table hiveTable = new org.apache.hadoop.hive.ql.metadata.Table(t);

    return new DrillHiveTable(name, this.getName(), selection, this.config, hiveTable);

  }

  @Override
  public Set<String> getTableNames() {
    try{
    List<String> dbTables = hiveSchema.getHiveDb().getAllTables(name);
    return new HashSet<String>(dbTables);
    } catch (HiveException e) {
      logger.error("Failure while attempting to get Hive Table names.", e);
      return Collections.emptySet();
    }finally{}
  }

  @Override
  public Set<String> getTableFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public boolean isMutable() {
    return false;
  }

}
