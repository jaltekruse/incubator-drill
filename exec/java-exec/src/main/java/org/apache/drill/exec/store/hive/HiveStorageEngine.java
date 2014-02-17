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
package org.apache.drill.exec.store.hive;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStorageEngine;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class HiveStorageEngine extends AbstractStorageEngine {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveStorageEngine.class);
  
  private HiveStorageEngineConfig config;
  private HiveConf hiveConf;
  private HiveSchemaProvider schemaProvider;
  static private DrillbitContext context;

  public HiveStorageEngine(HiveStorageEngineConfig config, DrillbitContext context) throws ExecutionSetupException {
    this.config = config;
    this.context = context;
    this.schemaProvider = new HiveSchemaProvider(config, context.getConfig());
    this.hiveConf = config.getHiveConf();
  }

  public HiveStorageEngineConfig getConfig() {
    return config;
  }

  public DrillbitContext getContext() {
    return context;
  }

  @Override
  public HiveScan getPhysicalScan(Scan scan) throws IOException {
    HiveReadEntry hiveReadEntry = scan.getSelection().getListWith(new ObjectMapper(), new TypeReference<HiveReadEntry>(){});
    try {
      return new HiveScan(hiveReadEntry, this, null);
    } catch (ExecutionSetupException e) {
      throw new IOException(e);
    }
  }

  List<String> getPartitions(String dbName, String tableName) throws TException {
    List<Partition> partitions = schemaProvider.getMetaClient().listPartitions(dbName, tableName, Short.MAX_VALUE);
    List<String> partitionLocations = Lists.newArrayList();
    if (partitions == null) return null;
    for (Partition part : partitions) {
      partitionLocations.add(part.getSd().getLocation());
    }
    return partitionLocations;
  }

  public static class HiveEntry{

    private Table table;

    public HiveEntry(Table table) {
      this.table = table;
    }

    public Table getTable() {
      return table;
    }

  }

  public static class HiveSchemaProvider{

    private HiveConf hiveConf;
    private HiveMetaStoreClient metaClient;

    public HiveSchemaProvider(HiveStorageEngineConfig config, DrillConfig dConfig) throws ExecutionSetupException {
      hiveConf = config.getHiveConf();
    }

    public HiveMetaStoreClient getMetaClient() throws MetaException {
      if (metaClient == null) {
        metaClient = new HiveMetaStoreClient(hiveConf);
      }
      return metaClient;
    }

    public Table getTable(String dbName, String tableName) throws TException {
      HiveMetaStoreClient mClient = getMetaClient();
      try {
        return  mClient.getTable(dbName, tableName);
      }catch (NoSuchObjectException e) {
        logger.error("Database: {} table: {} not found", dbName, tableName);
        throw new RuntimeException(e);
      } catch (TException e) {
        mClient.reconnect();
        return  mClient.getTable(dbName, tableName);
      }
    }

    List<Partition> getPartitions(String dbName, String tableName) throws TException {
      HiveMetaStoreClient mClient = getMetaClient();
      List<Partition> partitions;
      try {
        partitions = getMetaClient().listPartitions(dbName, tableName, Short.MAX_VALUE);
      } catch (TException e) {
        mClient.reconnect();
        partitions = getMetaClient().listPartitions(dbName, tableName, Short.MAX_VALUE);
      }
      return partitions;
    }

    
    public HiveReadEntry getSelectionBaseOnName(String name) {
      String[] dbNameTableName = name.split("\\.");
      String dbName;
      String t;
      if (dbNameTableName.length > 1) {
        dbName = dbNameTableName[0];
        t = dbNameTableName[1];
      } else {
        dbName = "default";
        t = name;
      }

      try {
        Table table = getTable(dbName, t);
        List<Partition> partitions = getPartitions(dbName, t);
        List<HiveTable.HivePartition> hivePartitions = Lists.newArrayList();
        for(Partition part : partitions) {
          hivePartitions.add(new HiveTable.HivePartition(part));
        }
        if (hivePartitions.size() == 0) hivePartitions = null;
        return new HiveReadEntry(new HiveTable(table), hivePartitions);
      } catch (NoSuchObjectException e) {
        throw new DrillRuntimeException(e);
      } catch (TException e) {
        throw new DrillRuntimeException(e);
      }
    }
  }
}
