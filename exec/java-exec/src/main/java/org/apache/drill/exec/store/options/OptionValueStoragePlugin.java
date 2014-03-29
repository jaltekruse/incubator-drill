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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaHolder;

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
  public OptionValueGroupScan getPhysicalScan(Scan scan) throws IOException {
    return new OptionValueGroupScan();
  }

  @Override
  public Schema createAndAddSchema(SchemaPlus parent) {
    Schema s = new ISchema(parent);
    parent.add(s);
    return s;
  }

  private class ISchema extends AbstractSchema{
    OptionValueDrillTable TABLE_INSTANCE = new OptionValueDrillTable("OPTIONS", new OptionValueTable(), config);

    public ISchema(SchemaPlus parent){
      super(new SchemaHolder(parent), "OPTIONS");
    }

    @Override
    public DrillTable getTable(String name) {
      if (name.equals("OPTIONS")){
        return TABLE_INSTANCE;
      }
      else {
        throw new RuntimeException("Only table present in the OPTIONS schema is, OPTIONS.");
      }
    }

    @Override
    public Set<String> getTableNames() {
      return Sets.newHashSet("OPTIONS");
    }

  }
}
