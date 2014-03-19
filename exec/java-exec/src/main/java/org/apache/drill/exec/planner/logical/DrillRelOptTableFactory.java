/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.planner.logical;

import com.google.common.collect.Lists;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.data.Scan;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.util.ArrayList;
import java.util.List;

public class DrillRelOptTableFactory {

  public static final double DEFAULT_ROW_COUNT = 5000;

  private final List<String> qualifiedName = Lists.newArrayList("screen");
  private final RelDataType rowType = new RelDataTypeDrillImpl(new JavaTypeFactoryImpl());
  MockRelOptSchema relOptSchema = new MockRelOptSchema();
  List<RelCollation> relCollations = new ArrayList<>();

  public DrillRelOptTableFactory(){}

  public DrillScreenRelOptTable buildScreenTable() {
    DrillTable table = new DynamicDrillTable(null, null, null);
    DrillScreenRelOptTable ret =
        new DrillScreenRelOptTable(qualifiedName, DEFAULT_ROW_COUNT, rowType, relOptSchema, relCollations, table);
    relOptSchema.setRelOptTable(ret);
    return ret;
  }

  public DrillDynamicRelOptTable buildDynamicTable(Scan scan, StoragePluginConfig storagePluginConfig) {
    DrillTable table = new DynamicDrillTable(scan.getStorageEngine(), scan.getSelection(), storagePluginConfig);
    DrillDynamicRelOptTable ret =
        new DrillDynamicRelOptTable(qualifiedName, DEFAULT_ROW_COUNT, rowType, relOptSchema, relCollations, table,
                                    scan.getStorageEngine(), storagePluginConfig, scan.getSelection() );
    relOptSchema.setRelOptTable(ret);
    return ret;
  }

  private void setRelCollations(List<RelCollation> relCollations) {
    this.relCollations = relCollations;
  }

  public static class DrillScreenRelOptTable extends DrillRelOptTable {

    public DrillScreenRelOptTable(List<String> qualifiedName, double rowCount, RelDataType rowType,
                                  RelOptSchema relOptSchema, List<RelCollation> relCollations, DrillTable table){
      super(qualifiedName, rowCount, rowType, relOptSchema, relCollations, table);
    }
  }

  public static class DrillDynamicRelOptTable extends DrillScreenRelOptTable {

    private final String storageEngineName;
    public final StoragePluginConfig storageEngineConfig;
    private Object selection;

    public DrillDynamicRelOptTable(List<String> qualifiedName, double rowCount, RelDataType rowType,
                                   RelOptSchema relOptSchema, List<RelCollation> relCollations, DrillTable table,
                                   String storageEngineName, StoragePluginConfig storageEngineConfig,
                                   Object selection) {
      super(qualifiedName, rowCount, rowType, relOptSchema, relCollations, table);
      this.storageEngineName = storageEngineName;
      this.storageEngineConfig = storageEngineConfig;
      this.selection = selection;
    }
  }

  public static class MockRelOptSchema implements RelOptSchema {

    RelOptTable relOptTable;
    RelDataTypeFactory relDataTypeFactory;

    public MockRelOptSchema() {

    }

    public MockRelOptSchema(RelOptTable relOptTable) {
      this.relOptTable = relOptTable;
      this.relDataTypeFactory = new JavaTypeFactoryImpl();
    }

    void setRelOptTable(RelOptTable relOptTable){
      this.relOptTable = relOptTable;
    }

    /**
     * Assuming all 'members' share a single table provided in constructor.
     */
    @Override
    public RelOptTable getTableForMember(List<String> names) {
      return relOptTable;
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
      return relDataTypeFactory;
    }

    @Override
    public void registerRules(RelOptPlanner planner) throws Exception {
      // TODO - figure out if this needs to be filled in
    }
  }
}
