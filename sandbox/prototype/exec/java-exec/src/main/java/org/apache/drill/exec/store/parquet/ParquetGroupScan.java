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

import java.util.*;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntryFromHDFS;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.config.MockGroupScanPOP;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.store.StorageEngineRegistry;
import org.apache.drill.exec.store.AffinityCalculator;
import org.apache.drill.storage.ParquetStorageEngineConfig;
import org.apache.hadoop.fs.FileSystem;
import parquet.org.codehaus.jackson.annotate.JsonCreator;

@JsonTypeName("parquet-scan")
public class ParquetGroupScan extends AbstractGroupScan<ParquetGroupScan.RowGroupInfo> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockGroupScanPOP.class);

  private  LinkedList<RowGroupInfo>[] mappings;
  private List<RowGroupInfo> rowGroupInfos;

  private long totalBytes;
  private Collection<DrillbitEndpoint> availableEndpoints;
  private ParquetStorageEngine storageEngine;
  private StorageEngineRegistry engineRegistry;
  private FileSystem fs;
  private String fileName;

  @JsonCreator
  public ParquetGroupScan(@JsonProperty("entries") List<RowGroupInfo> logicalReadEntries,
                          @JsonProperty("storageengine") ParquetStorageEngineConfig storageEngineConfig,
                          @JacksonInject StorageEngineRegistry engineRegistry) throws SetupException {
    this.storageEngine = (ParquetStorageEngine) engineRegistry.getEngine(storageEngineConfig);
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.fs = storageEngine.getFileSystem();
    this.fileName = logicalReadEntries.get(0).getPath();
    this.engineRegistry = engineRegistry;
    AffinityCalculator ac = new AffinityCalculator(fileName, fs, availableEndpoints);
    for (RowGroupInfo e : logicalReadEntries) {
      ac.setEndpointBytes(e);
    }
  }

  public ParquetGroupScan(@JsonProperty("entries") List<RowGroupInfo> logicalReadEntries,
                          ParquetStorageEngine storageEngine) {
    this.storageEngine = storageEngine;
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.fs = storageEngine.getFileSystem();
    this.fileName = logicalReadEntries.get(0).getPath();
    this.engineRegistry = engineRegistry;
    AffinityCalculator ac = new AffinityCalculator(fileName, fs, availableEndpoints);
    for (RowGroupInfo e : logicalReadEntries) {
      ac.setEndpointBytes(e);
    }
  }

  public static class ParquetFileReadEntry {

    String path;

    public ParquetFileReadEntry(@JsonProperty String path){
      this.path = path;
    }
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  public static class RowGroupInfo extends ReadEntryFromHDFS {

    private HashMap<DrillbitEndpoint,Long> endpointBytes;
    private long maxBytes;

    @JsonCreator
    public RowGroupInfo(@JsonProperty("path") String path, @JsonProperty("start") long start, @JsonProperty("length") long length) {
      super(path, start, length);
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

    public HashMap<DrillbitEndpoint,Long> getEndpointBytes() {
      return endpointBytes;
    }

    public void setEndpointBytes(HashMap<DrillbitEndpoint,Long> endpointBytes) {
      this.endpointBytes = endpointBytes;
    }

    public void setMaxBytes(long bytes) {
      this.maxBytes = bytes;
    }

    public long getMaxBytes() {
      return maxBytes;
    }
  }

  private class ParquetReadEntryComparator implements Comparator<RowGroupInfo> {
    public int compare(RowGroupInfo e1, RowGroupInfo e2) {
      if (e1.getMaxBytes() == e2.getMaxBytes()) return 0;
      return (e1.getMaxBytes() > e2.getMaxBytes()) ? 1 : -1;
    }
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    LinkedList<EndpointAffinity> endpointAffinities = new LinkedList<>();
    for (RowGroupInfo entry : rowGroupInfos) {
      for (DrillbitEndpoint d : entry.getEndpointBytes().keySet()) {
        long bytes = entry.getEndpointBytes().get(d);
        float affinity = bytes / totalBytes;
        if (endpointAffinities.contains(d)) {
          endpointAffinities.get(endpointAffinities.indexOf(d)).addAffinity(affinity);
        } else {
          endpointAffinities.add(new EndpointAffinity(d, affinity));
        }
      }
    }
    return endpointAffinities;
  }

  @Override
  public List getReadEntries() {
    return null;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    Preconditions.checkArgument(endpoints.size() <= getReadEntries().size());

    Collections.sort(getReadEntries(), new ParquetReadEntryComparator());
    mappings = new LinkedList[endpoints.size()];
    LinkedList<RowGroupInfo> unassigned = scanAndAssign(endpoints, getReadEntries(), 100, true);
    LinkedList<RowGroupInfo> unassigned2 = scanAndAssign(endpoints, unassigned, 50, true);
    LinkedList<RowGroupInfo> unassigned3 = scanAndAssign(endpoints, unassigned, 25, true);
    LinkedList<RowGroupInfo> unassigned4 = scanAndAssign(endpoints, unassigned, 0, false);
    assert unassigned4.size() == 0 : String.format("All readEntries should be assigned by now, but some are still unassigned");
  }

  private LinkedList<RowGroupInfo> scanAndAssign (List<DrillbitEndpoint> endpoints, List<RowGroupInfo> readEntries, int requiredPercentage, boolean mustContain) {
    Collections.sort(getReadEntries(), new ParquetReadEntryComparator());
    LinkedList<RowGroupInfo> unassigned = new LinkedList<>();

    int maxEntries = (int) (getReadEntries().size() / endpoints.size() * 1.5);

    if (maxEntries < 1) maxEntries = 1;

    int i =0;
    for(RowGroupInfo e : readEntries) {
      boolean assigned = false;
      for (int j = i; j < i + endpoints.size(); j++) {
        DrillbitEndpoint currentEndpoint = endpoints.get(j%endpoints.size());
        if ((e.getEndpointBytes().containsKey(currentEndpoint) || !mustContain) &&
                (mappings[j%endpoints.size()] == null || mappings[j%endpoints.size()].size() < maxEntries) &&
                e.getEndpointBytes().get(currentEndpoint) >= e.getMaxBytes() * requiredPercentage / 100) {
          LinkedList<RowGroupInfo> entries = mappings[j%endpoints.size()];
          if(entries == null){
            entries = new LinkedList<RowGroupInfo>();
            mappings[j%endpoints.size()] = entries;
          }
          entries.add(e);
          assigned = true;
          break;
        }
      }
      if (!assigned) unassigned.add(e);
      i++;
    }
    return unassigned;
  }

  @Override
  public GroupScan<?> getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.length : String.format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.length, minorFragmentId);
    return new ParquetGroupScan(mappings[minorFragmentId], storageEngine);
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
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetGroupScan(rowGroupInfos, storageEngine);
  }

}
