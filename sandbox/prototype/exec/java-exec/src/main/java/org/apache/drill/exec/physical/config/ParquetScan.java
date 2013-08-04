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
package org.apache.drill.exec.physical.config;

import java.util.*;

import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntryFromHDFS;
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.drill.exec.store.AffinityCalculator;
import org.apache.hadoop.fs.FileSystem;

import static org.apache.drill.exec.physical.config.ParquetRowGroupScan.*;

@JsonTypeName("parquet-scan")
public class ParquetScan extends AbstractScan<ParquetScan.RowGroupInfo> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockScanPOP.class);

  private  LinkedList<ParquetRowGroupReadEntry>[] mappings;
  private long totalBytes;
  private Collection<DrillbitEndpoint> availableEndpoints;
  private ParquetStorageEngine storageEngine;
  private FileSystem fs;
  private String fileName;
  private LinkedList<RowGroupInfo> rowGroups;
// TODO we should not be using RowGroupInfo to construct ParquetScan, should we?
  @JsonCreator
  public ParquetScan(@JsonProperty("entries") List<RowGroupInfo> readEntries) {
    super(readEntries);
  }

  public LinkedList<RowGroupInfo> getRowGroups() {
    return this.rowGroups;
  }

  public void setRowGroups(LinkedList<RowGroupInfo> rowGroups) {
    this.rowGroups = rowGroups;
  }

  public ParquetScan(@JsonProperty("entries") List<RowGroupInfo> readEntries, ParquetStorageEngine storageEngine) {
    super(readEntries);
    this.storageEngine = storageEngine;
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.fs = storageEngine.getFileSystem();
    this.fileName = readEntries.get(0).getPath();
    AffinityCalculator ac = new AffinityCalculator(fileName, fs, availableEndpoints);
    for (RowGroupInfo e : readEntries) {
      ac.setEndpointBytes(e);
    }
  }

//TODO remove this
  @Override
  @JsonIgnore
  public List<RowGroupInfo> getReadEntries() {
    return super.getReadEntries();
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

    public ParquetRowGroupReadEntry getRowGroupReadEntry() {
      return new ParquetRowGroupReadEntry(this.getPath(), this.getStart(), this.getLength());
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
    for (RowGroupInfo entry : readEntries) {
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

  @SuppressWarnings("unchecked")
  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    Preconditions.checkArgument(endpoints.size() <= getReadEntries().size());

    Collections.sort(getReadEntries(), new ParquetReadEntryComparator());
    mappings = new LinkedList[endpoints.size()];
    LinkedList<RowGroupInfo> unassigned = scanAndAssign(endpoints, getReadEntries(), 100, true);
    LinkedList<RowGroupInfo> unassigned2 = scanAndAssign(endpoints, unassigned, 50, true);
    LinkedList<RowGroupInfo> unassigned3 = scanAndAssign(endpoints, unassigned2, 25, true);
    LinkedList<RowGroupInfo> unassigned4 = scanAndAssign(endpoints, unassigned3, 0, false);
    assert unassigned4.size() == 0 : String.format("All readEntries should be assigned by now, but some are still unassigned");
  }

  private LinkedList<RowGroupInfo> scanAndAssign (List<DrillbitEndpoint> endpoints, List<RowGroupInfo> rowGroups, int requiredPercentage, boolean mustContain) {
    Collections.sort(getReadEntries(), new ParquetReadEntryComparator());
    LinkedList<RowGroupInfo> unassigned = new LinkedList<>();

    int maxEntries = (int) (getRowGroups().size() / endpoints.size() * 1.5);

    if (maxEntries < 1) maxEntries = 1;

    int i =0;
    for(RowGroupInfo e : rowGroups) {
      boolean assigned = false;
      for (int j = i; j < i + endpoints.size(); j++) {
        DrillbitEndpoint currentEndpoint = endpoints.get(j%endpoints.size());
        if ((e.getEndpointBytes().containsKey(currentEndpoint) || !mustContain) &&
                (mappings[j%endpoints.size()] == null || mappings[j%endpoints.size()].size() < maxEntries) &&
                e.getEndpointBytes().get(currentEndpoint) >= e.getMaxBytes() * requiredPercentage / 100) {
          LinkedList<ParquetRowGroupScan.ParquetRowGroupReadEntry> entries = mappings[j%endpoints.size()];
          if(entries == null){
            entries = new LinkedList<ParquetRowGroupScan.ParquetRowGroupReadEntry>();
            mappings[j%endpoints.size()] = entries;
          }
          entries.add(e.getRowGroupReadEntry());
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
  public Scan<?> getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.length : String.format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.length, minorFragmentId);
    return new ParquetRowGroupScan(mappings[minorFragmentId]);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetScan(readEntries);

  }

}
