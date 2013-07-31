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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.ReadEntry;
<<<<<<< HEAD
import org.apache.drill.exec.physical.ReadEntryFromHDFS;
=======
>>>>>>> Parquet reader is now hooked up to the full execution engine and can go through a scan/screen. Tests have been updated to validate all of the individual values read out of parquet and sent through a scan/screen plan in the full execution engine.
import org.apache.drill.exec.physical.base.AbstractScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Scan;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
<<<<<<< HEAD

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.BlockLocation;

@JsonTypeName("parquet-scan")
public class ParquetScan extends AbstractScan<ParquetScan.ParquetReadEntry> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockScanPOP.class);

  private  LinkedList<ParquetReadEntry>[] mappings;

  @JsonCreator
  public ParquetScan(@JsonProperty("entries") List<ParquetReadEntry> readEntries) {
    super(readEntries);
  }

  public static class EndpointBytes {

    private DrillbitEndpoint endpoint;
    private long bytes = 0;

    public EndpointBytes(DrillbitEndpoint endpoint) {
      super();
      this.endpoint = endpoint;
    }

    public EndpointBytes(DrillbitEndpoint endpoint, long bytes) {
      super();
      this.endpoint = endpoint;
      this.bytes = bytes;
    }

    public DrillbitEndpoint getEndpoint() {
      return endpoint;
    }

    public void setEndpoint(DrillbitEndpoint endpoint) {
      this.endpoint = endpoint;
    }
    public long getAffinity() {
      return bytes;
    }
  }

  public static class ParquetReadEntry extends ReadEntryFromHDFS {

    private EndpointBytes[] endpointBytes;

    @JsonCreator
    public ParquetReadEntry(@JsonProperty("path") String path,@JsonProperty("start") long start,@JsonProperty("length") long length) {
      super(path, start, length);
=======
import org.apache.drill.exec.vector.TypeHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("parquet-scan")
public class ParquetScan extends AbstractScan<ParquetScan.ScanEntry> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockScanPOP.class);

  private  LinkedList<ScanEntry>[] mappings;

  @JsonCreator
  public ParquetScan(@JsonProperty("entries") List<ScanEntry> readEntries) {
    super(readEntries);
  }

  public static class ScanEntry implements ReadEntry {

    private String filename;

    @JsonCreator
    public ScanEntry(@JsonProperty("filename") String filename) {
      this.filename = filename;
>>>>>>> Parquet reader is now hooked up to the full execution engine and can go through a scan/screen. Tests have been updated to validate all of the individual values read out of parquet and sent through a scan/screen plan in the full execution engine.
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

    public EndpointBytes[] getEndpointBytes() {
      return endpointBytes;
    }

    public void setEndpointBytes(EndpointBytes[] endpointBytes) {
      this.endpointBytes = endpointBytes;
    }
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) {
    Preconditions.checkArgument(endpoints.size() <= getReadEntries().size());

    mappings = new LinkedList[endpoints.size()];

    int i =0;
    for(ParquetReadEntry e : this.getReadEntries()){
      if(i == endpoints.size()) i -= endpoints.size();
      LinkedList<ParquetReadEntry> entries = mappings[i];
      if(entries == null){
        entries = new LinkedList<ParquetReadEntry>();
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
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetScan(readEntries);

  }

}
