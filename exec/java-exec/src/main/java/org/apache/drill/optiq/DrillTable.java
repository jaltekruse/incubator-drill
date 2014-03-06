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
package org.apache.drill.optiq;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema.TableType;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.Table;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;

/** Optiq Table used by Drill. */
public class DrillTable implements Table, RelOptTable{
  
  private final String name;
  private final String storageEngineName;  
  public final StorageEngineConfig storageEngineConfig;
  private Object selection;
  
  
  /** Creates a DrillTable. */
  public DrillTable(String name, String storageEngineName, Object selection, StorageEngineConfig storageEngineConfig) {
    this.name = name;
    this.selection = selection;
    this.storageEngineConfig = storageEngineConfig;
    this.storageEngineName = storageEngineName;
  }
  
  public String getName() {
    return name;
  }

  public StorageEngineConfig getStorageEngineConfig(){
    return storageEngineConfig;
  }
  
  public Object getSelection() {
    return selection;
  }
  
  public String getStorageEngineName() {
    return storageEngineName;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable table) {
    return new DrillScanRel(context.getCluster(),
        context.getCluster().traitSetOf(DrillRel.CONVENTION),
        table);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return new RelDataTypeDrillImpl(typeFactory);
  }

  @Override
  public TableType getJdbcTableType() {
    return null;
  }

  @Override
  public List<String> getQualifiedName() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public double getRowCount() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public RelDataType getRowType() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public RelOptSchema getRelOptSchema() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public RelNode toRel(ToRelContext context) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<RelCollation> getCollationList() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean isKey(BitSet columns) {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (clazz == this.getClass())
      return (T) this;
    else return null;
  }

  @Override
  public Expression getExpression(Class clazz) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }


//  /** Factory for custom tables in Optiq schema. */
//  @SuppressWarnings("UnusedDeclaration")
//  public static class Factory implements TableFactory<DrillTable> {
//
//    @Override
//    public DrillTable create(Schema schema, String name, Map<String, Object> operand, RelDataType rowType) {
//      
//      final ClasspathRSE.ClasspathRSEConfig rseConfig = new ClasspathRSE.ClasspathRSEConfig();
//      final ClasspathInputConfig inputConfig = new ClasspathInputConfig();
//      inputConfig.path = "/" + name.toLowerCase() + ".json";
//      inputConfig.type = DataWriter.ConverterType.JSON;
//      return createTable(schema.getTypeFactory(), (MutableSchema) schema, name, "donuts-json", rseConfig, inputConfig);
//    }
//  }

  
  
}