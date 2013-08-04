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

import com.fasterxml.jackson.annotation.*;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntryFromHDFS;
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.config.MockScanPOP;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.store.StorageEngineRegistry;

@JsonTypeName("parquet-scan")
public class ParquetScan extends AbstractScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockScanPOP.class);

  private  LinkedList<RowGroupInfo>[] mappings;
  private List<ParquetFileReadEntry> logicalReadEntries;

  @JsonCreator
  public ParquetScan(@JsonProperty("entries") List<ParquetFileReadEntry> logicalReadEntries,
                     @JacksonInject StorageEngineRegistry engineRegistry) {

  }


  public static class ParquetFileReadEntry {

    String path;

    public ParquetFileReadEntry(@JsonProperty String path){
      this.path = path;
    }

  }

  public static class RowGroupInfo extends ReadEntryFromHDFS {

    private HashMap<DrillbitEndpoint, Integer> endpointBytes;

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

    public Map<DrillbitEndpoint, Integer> getEndpointBytes() {
      return endpointBytes;
    }

  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

  @Override
  public List getReadEntries() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @SuppressWarnings("unchecked")
  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    Preconditions.checkArgument(endpoints.size() <= getReadEntries().size());

    mappings = new LinkedList[endpoints.size()];

    int i =0;
    for(RowGroupInfo e : this.getReadEntries()){
      if(i == endpoints.size()) i -= endpoints.size();
      LinkedList<RowGroupInfo> entries = mappings[i];
      if(entries == null){
        entries = new LinkedList<RowGroupInfo>();
        mappings[i] = entries;
      }
      entries.add(e);
      i++;
    }
  }

  @Override
  public Scan<?> getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.length : String.format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.length, minorFragmentId);
    return new ParquetScan(mappings[minorFragmentId]);
  }

  @Override
  public OperatorCost getCost() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Size getSize() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetScan(readEntries);

  }

}
