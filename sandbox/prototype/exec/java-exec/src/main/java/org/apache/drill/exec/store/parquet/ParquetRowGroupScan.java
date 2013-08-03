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
package org.apache.drill.exec.store.parquet;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.graph.GraphVisitor;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.base.*;
import org.apache.drill.exec.store.StorageEngineRegistry;

import java.util.Iterator;
import java.util.List;

// Class containing information for reading a single parquet row group form HDFS
@JsonTypeName("parquet-row-group-scan")
public class ParquetRowGroupScan implements SubScan<ParquetRowGroupScan.RowGroupReadEntry> {

  private StorageEngineConfig engineCofig;
  private ParquetStorageEngine parquetStorageEngine;
  private List<RowGroupReadEntry> rowGroupReadEntries;

  @JsonCreator
  public ParquetRowGroupScan(@JacksonInject StorageEngineRegistry registry, @JsonProperty StorageEngineConfig engineConfig,
                             @JsonProperty List<RowGroupReadEntry> rowGroupReadEntries) throws SetupException {
    parquetStorageEngine = (ParquetStorageEngine) registry.getEngine(engineConfig);
    this.rowGroupReadEntries = rowGroupReadEntries;
  }

  public List<RowGroupReadEntry> getRowGroupReadEntries() {
    return rowGroupReadEntries;
  }

  @Override
  public OperatorCost getCost() {
    return null;
  }

  @Override
  public Size getSize() {
    return null;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return null;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return null;
  }

  @Override
  public void accept(GraphVisitor<PhysicalOperator> visitor) {
  
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return null;
  }

  public static class RowGroupReadEntry implements ReadEntry{

    @Override
    public OperatorCost getCost() {
      return null; 
    }

    @Override
    public Size getSize() {
      return null; 
    }
  }
}
