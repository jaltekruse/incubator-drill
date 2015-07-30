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

import java.util.List;
import java.util.Map;

import org.apache.drill.exec.store.PartitionDescriptors;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;

@JsonTypeName("table")
public class HiveTable {

  @JsonIgnore
  private Table table;

  @JsonProperty
  public String tableName;
  @JsonProperty
  public String dbName;
  @JsonProperty
  public String owner;
  @JsonProperty
  public int createTime;
  @JsonProperty
  public int lastAccessTime;
  @JsonProperty
  public int retention;
  @JsonProperty
  public PartitionDescriptors.StorageDescriptorWrapper sd;
  @JsonProperty
  public List<PartitionDescriptors.FieldSchemaWrapper> partitionKeys;
  @JsonProperty
  public Map<String,String> parameters;
  @JsonProperty
  public String viewOriginalText;
  @JsonProperty
  public String viewExpandedText;
  @JsonProperty
  public String tableType;

  @JsonCreator
  public HiveTable(@JsonProperty("tableName") String tableName, @JsonProperty("dbName") String dbName, @JsonProperty("owner") String owner, @JsonProperty("createTime") int createTime,
                   @JsonProperty("lastAccessTime") int lastAccessTime, @JsonProperty("retention") int retention, @JsonProperty("sd") PartitionDescriptors.StorageDescriptorWrapper sd,
                   @JsonProperty("partitionKeys") List<PartitionDescriptors.FieldSchemaWrapper> partitionKeys, @JsonProperty("parameters") Map<String, String> parameters,
                   @JsonProperty("viewOriginalText") String viewOriginalText, @JsonProperty("viewExpandedText") String viewExpandedText, @JsonProperty("tableType") String tableType
                   ) {
    this.tableName = tableName;
    this.dbName = dbName;
    this.owner = owner;
    this.createTime = createTime;
    this.lastAccessTime = lastAccessTime;
    this.retention = retention;
    this.sd = sd;
    this.partitionKeys = partitionKeys;
    this.parameters = parameters;
    this.viewOriginalText = viewOriginalText;
    this.viewExpandedText = viewExpandedText;
    this.tableType = tableType;

    List<FieldSchema> partitionKeysUnwrapped = Lists.newArrayList();
    for (PartitionDescriptors.FieldSchemaWrapper w : partitionKeys) {
      partitionKeysUnwrapped.add(w.getFieldSchema());
    }
    StorageDescriptor sdUnwrapped = sd.getSd();
    this.table = new Table(tableName, dbName, owner, createTime, lastAccessTime, retention, sdUnwrapped, partitionKeysUnwrapped,
            parameters, viewOriginalText, viewExpandedText, tableType);
  }

  public HiveTable(Table table) {
    if (table == null) {
      return;
    }
    this.table = table;
    this.tableName = table.getTableName();
    this.dbName = table.getDbName();
    this.owner = table.getOwner();
    this.createTime = table.getCreateTime();
    this.lastAccessTime = table.getLastAccessTime();
    this.retention = table.getRetention();
    this.sd = new PartitionDescriptors.StorageDescriptorWrapper(table.getSd());
    this.partitionKeys = Lists.newArrayList();
    for (FieldSchema f : table.getPartitionKeys()) {
      this.partitionKeys.add(new PartitionDescriptors.FieldSchemaWrapper(f));
    }
    this.parameters = table.getParameters();
    this.viewOriginalText = table.getViewOriginalText();
    this.viewExpandedText = table.getViewExpandedText();
    this.tableType = table.getTableType();
  }

  @JsonIgnore
  public Table getTable() {
    return table;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Table(");

    sb.append("dbName:");
    sb.append(this.dbName);
    sb.append(", ");

    sb.append("tableName:");
    sb.append(this.tableName);
    sb.append(")");

    return sb.toString();
  }

}
