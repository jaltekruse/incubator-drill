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

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.drill.common.graph.GraphVisitor;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.ReadEntryFromHDFS;
import org.apache.drill.exec.physical.base.*;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StorageEngineRegistry;
import org.apache.drill.storage.ParquetStorageEngineConfig;

import java.util.*;

// Class containing information for reading a single parquet row group form HDFS
@JsonTypeName("parquet-row-group-scan")
public class ParquetRowGroupScan extends AbstractBase implements SubScan {

  private StorageEngineConfig engineCofig;
  private ParquetStorageEngine parquetStorageEngine;
  private List<RowGroupReadEntry> rowGroupReadEntries;

  @JsonCreator
  public ParquetRowGroupScan(@JacksonInject StorageEngineRegistry registry, @JsonProperty("engine-config") StorageEngineConfig engineConfig,
                             @JsonProperty("read-entries") LinkedList<RowGroupReadEntry> rowGroupReadEntries) throws SetupException {
    parquetStorageEngine = (ParquetStorageEngine) registry.getEngine(engineConfig);
    this.rowGroupReadEntries = rowGroupReadEntries;
  }

  public ParquetRowGroupScan( ParquetStorageEngine engine, ParquetStorageEngineConfig config,
                              List<RowGroupReadEntry> rowGroupReadEntries) throws SetupException {
    parquetStorageEngine = engine;
    engineCofig = config;
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
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    try {
      return new ParquetRowGroupScan(parquetStorageEngine, (ParquetStorageEngineConfig) engineCofig, rowGroupReadEntries);
    } catch (SetupException e) {
      // TODO - handle this
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return null;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class RowGroupReadEntry extends ReadEntryFromHDFS {

    private int rowGroupIndex;
    private String DFSname;

    @parquet.org.codehaus.jackson.annotate.JsonCreator
    public RowGroupReadEntry(@JsonProperty("path") String path, @JsonProperty("start") long start,
                             @JsonProperty("length") long length, @JsonProperty("rowGroupIndex") int rowGroupIndex,
                             @JsonProperty("DFSname") String DFSname) {
      super(path, start, length);
      this.DFSname = DFSname;
      this.rowGroupIndex = rowGroupIndex;
    }

    @Override
    public OperatorCost getCost() {
      return new OperatorCost(1, 2, 1, 1);
    }

    @Override
    public Size getSize() {
      // TODO - these values are wrong, I cannot know these until after I read a file
      return new Size(10, 10);
    }

    @JsonIgnore
    public RowGroupReadEntry getRowGroupReadEntry() {
      return new RowGroupReadEntry(this.getPath(), this.getStart(), this.getLength(), this.rowGroupIndex, this.getDFSname());
    }

    public int getRowGroupIndex(){
      return rowGroupIndex;
    }

    public String getDFSname() {
      return DFSname;
    }
  }

}
