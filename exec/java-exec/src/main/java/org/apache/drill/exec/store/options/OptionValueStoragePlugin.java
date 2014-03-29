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
package org.apache.drill.exec.store.options;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.rpc.user.DrillUser;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.ischema.SelectedTable;

public class OptionValueStoragePlugin extends AbstractStoragePlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OptionValueStoragePlugin.class);

  private final OptionValueStorageConfig config;
  private final DrillbitContext context;
  private final String name;

  public OptionValueStoragePlugin(OptionValueStorageConfig config, DrillbitContext context, String name){
    this.config = config;
    this.context = context;
    this.name = name;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public OptionValueGroupScan getPhysicalScan(JSONOptions selection) throws IOException {
    return new OptionValueGroupScan();
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public void registerSchemas(DrillUser user, SchemaPlus parent) {
    AbstractSchema s = new ISchema(parent, this);
    parent.add(s.getName(), s);
  }

  private class ISchema extends AbstractSchema {
    private Map<String, OptionValueDrillTable> tables;
    public ISchema(SchemaPlus parent, OptionValueStoragePlugin plugin){
      super("INFORMATION_SCHEMA");
      Map<String, OptionValueDrillTable> tbls = Maps.newHashMap();
      for(SelectedTable tbl : SelectedTable.values()){
        tbls.put(tbl.name(), new OptionValueDrillTable("OPTIONS", tbl, plugin));
      }
      this.tables = ImmutableMap.copyOf(tbls);
    }

    @Override
    public DrillTable getTable(String name) {
      return tables.get(name);
    }

    @Override
    public Set<String> getTableNames() {
      return tables.keySet();
    }

  }
}
