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
public class ParquetGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockGroupScanPOP.class);

  @JsonIgnore
  private LinkedList<ParquetRowGroupScan.RowGroupReadEntry>[] mappings;
  @JsonIgnore
  private int mappingsPointer;

  private List<RowGroupInfo> rowGroupInfos;

  @JsonIgnore
  private long totalBytes;
  @JsonIgnore
  private Collection<DrillbitEndpoint> availableEndpoints;
  @JsonIgnore
  private ParquetStorageEngine storageEngine;
  @JsonIgnore
  private StorageEngineRegistry engineRegistry;
  private ParquetStorageEngineConfig engineConfig;
  @JsonIgnore
  private FileSystem fs;
  private String fileName;
  private LinkedList<RowGroupInfo> rowGroups;
  @JsonCreator
  public ParquetGroupScan(@JsonProperty("entries") List<RowGroupInfo> rowGroupInfos,
                          @JsonProperty("storageengine") ParquetStorageEngineConfig storageEngineConfig,
                          @JacksonInject StorageEngineRegistry engineRegistry) throws SetupException {
    this.storageEngine = (ParquetStorageEngine) engineRegistry.getEngine(storageEngineConfig);
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.fs = storageEngine.getFileSystem();
    this.engineConfig = storageEngineConfig;
    this.fileName = rowGroupInfos.get(0).getPath();
    this.engineRegistry = engineRegistry;
    this.rowGroupInfos = rowGroupInfos;
    AffinityCalculator ac = new AffinityCalculator(fileName, fs, availableEndpoints);
    for (RowGroupInfo e : rowGroupInfos) {
      ac.setEndpointBytes(e);
      totalBytes += e.getLength();
    }
  }

  public ParquetGroupScan(@JsonProperty("entries") List<RowGroupInfo> rowGroupInfos,
                          ParquetStorageEngine storageEngine) {
    this.storageEngine = storageEngine;
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.fs = storageEngine.getFileSystem();
    this.fileName = rowGroupInfos.get(0).getPath();
    this.rowGroupInfos = rowGroupInfos;
    this.engineRegistry = engineRegistry;
    AffinityCalculator ac = new AffinityCalculator(fileName, fs, availableEndpoints);
    for (RowGroupInfo e : rowGroupInfos) {
      ac.setEndpointBytes(e);
      totalBytes += e.getLength();
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
    private int rowGroupIndex;

    @JsonCreator
    public RowGroupInfo(@JsonProperty("path") String path, @JsonProperty("start") long start,
                        @JsonProperty("length") long length, @JsonProperty("rowGroupIndex") int rowGroupIndex) {
      super(path, start, length);
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

    public ParquetRowGroupScan.RowGroupReadEntry getRowGroupReadEntry() {
      return new ParquetRowGroupScan.RowGroupReadEntry(this.getPath(), this.getStart(), this.getLength(), this.rowGroupIndex);
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
    //LinkedList<EndpointAffinity> endpointAffinities = new LinkedList<>();
    HashMap<DrillbitEndpoint,EndpointAffinity> endpointAffinitiesMap = new HashMap();
    for (RowGroupInfo entry : rowGroupInfos) {
      for (DrillbitEndpoint d : entry.getEndpointBytes().keySet()) {
        long bytes = entry.getEndpointBytes().get(d);
        float affinity = (float)bytes / (float)totalBytes;
        if (endpointAffinitiesMap.containsKey(d)) {
          endpointAffinitiesMap.get(d).addAffinity(affinity);
        } else {
          endpointAffinitiesMap.put(d, new EndpointAffinity(d, affinity));
        }
      }
    }
    LinkedList<EndpointAffinity> endpointAffinities = new LinkedList();
    for (EndpointAffinity affinity : endpointAffinitiesMap.values()) {
      endpointAffinities.add(affinity);
    }
    return endpointAffinities;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    Preconditions.checkArgument(endpoints.size() <= rowGroupInfos.size());

    Collections.sort(rowGroupInfos, new ParquetReadEntryComparator());
    mappings = new LinkedList[endpoints.size()];
    LinkedList<RowGroupInfo> unassigned = scanAndAssign(endpoints, rowGroupInfos, 100, true, false);
    LinkedList<RowGroupInfo> unassigned2 = scanAndAssign(endpoints, unassigned, 50, true, false);
    LinkedList<RowGroupInfo> unassigned3 = scanAndAssign(endpoints, unassigned2, 25, true, false);
    LinkedList<RowGroupInfo> unassigned4 = scanAndAssign(endpoints, unassigned3, 0, false, false);
    LinkedList<RowGroupInfo> unassigned5 = scanAndAssign(endpoints, unassigned4, 0, false, true);
    assert unassigned5.size() == 0 : String.format("All readEntries should be assigned by now, but some are still unassigned");
  }

  private LinkedList<RowGroupInfo> scanAndAssign (List<DrillbitEndpoint> endpoints, List<RowGroupInfo> rowGroups, int requiredPercentage, boolean mustContain, boolean assignAll) {
    Collections.sort(rowGroupInfos, new ParquetReadEntryComparator());
    LinkedList<RowGroupInfo> unassigned = new LinkedList<>();

    int maxEntries = (int) ((float)rowGroupInfos.size() * 1.5 / endpoints.size());

    if (maxEntries < 1) maxEntries = 1;

    for(RowGroupInfo e : rowGroups) {
      boolean assigned = false;
      for (int j = mappingsPointer; j < mappingsPointer + endpoints.size(); j++) {
        DrillbitEndpoint currentEndpoint = endpoints.get(j%endpoints.size());
        if (assignAll || e.getEndpointBytes().size() > 0) {
          if (assignAll || e.getEndpointBytes().containsKey(currentEndpoint) || !mustContain) {
            if (assignAll || mappings[j%endpoints.size()] == null || mappings[j%endpoints.size()].size() < maxEntries) {
              if (assignAll || e.getEndpointBytes().get(currentEndpoint) != null && e.getEndpointBytes().get(currentEndpoint)  >= e.getMaxBytes() * requiredPercentage / 100) {
                LinkedList<ParquetRowGroupScan.RowGroupReadEntry> entries = mappings[j%endpoints.size()];
                if(entries == null){
                  entries = new LinkedList<ParquetRowGroupScan.RowGroupReadEntry>();
                  mappings[j%endpoints.size()] = entries;
                }
                entries.add(e.getRowGroupReadEntry());
                assigned = true;
                break;
              }
            }
          }
        }
      }
      if (!assigned) unassigned.add(e);
      mappingsPointer++;
    }
    return unassigned;
  }

  @Override
  public ParquetRowGroupScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.length : String.format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.length, minorFragmentId);
    try {
      return new ParquetRowGroupScan(storageEngine, engineConfig, mappings[minorFragmentId]);
    } catch (SetupException e) {
      e.printStackTrace(); // TODO - fix this
    }
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return rowGroupInfos.size();
  }

  @Override
  public OperatorCost getCost() {
    return new OperatorCost(1,1,1,1);
  }

  @Override
  public Size getSize() {
    // TODO - this is wrong, need to populate correctly
    return new Size(10,10);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetGroupScan(rowGroupInfos, storageEngine);
  }

}
