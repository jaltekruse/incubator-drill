package org.apache.drill.exec.store;


import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

public class AffinityCalculator {

  BlockLocation[] blocks;
  ImmutableRangeMap<Long,BlockLocation> blockMap;
  FileSystem fs;
  String fileName;
  Collection<DrillbitEndpoint> endpoints;
  HashMap<String,DrillbitEndpoint> endPointMap;

  public AffinityCalculator(String fileName, FileSystem fs, Collection<DrillbitEndpoint> endpoints) {
    this.fs = fs;
    this.fileName = fileName;
    this.endpoints = endpoints;
    buildBlockMap();
    buildEndpointMap();
  }

  private void buildBlockMap() {
    try {
      FileStatus file = fs.getFileStatus(new Path(fileName));
      blocks = fs.getFileBlockLocations(file, 0 , file.getLen());
    } catch (IOException ioe) { /* TODO Handle this */ }
    ImmutableRangeMap.Builder<Long, BlockLocation> blockMapBuilder = new ImmutableRangeMap.Builder<Long,BlockLocation>();
    for (BlockLocation block : blocks) {
      long start = block.getOffset();
      long end = start + block.getLength();
      Range<Long> range = Range.closedOpen(start, end);
      blockMapBuilder = blockMapBuilder.put(range, block);
    }
    blockMap = blockMapBuilder.build();
  }
  /**
   *
   * @param entry
   */
  public void setEndpointBytes(ParquetGroupScan.RowGroupInfo entry) {
    HashMap<String,Long> hostMap = new HashMap<>();
    long start = entry.getStart();
    long end = start + entry.getLength();
    Range<Long> entryRange = Range.closedOpen(start, end);
    ImmutableRangeMap<Long,BlockLocation> subRangeMap = blockMap.subRangeMap(entryRange);
    for (Map.Entry<Range<Long>,BlockLocation> e : subRangeMap.asMapOfRanges().entrySet()) {
      String[] hosts = null;
      Range<Long> blockRange = e.getKey();
      try {
        hosts = e.getValue().getHosts();
      } catch (IOException ioe) { /*TODO Handle this exception */}
      Range<Long> intersection = entryRange.intersection(blockRange);
      long bytes = intersection.upperEndpoint() - intersection.lowerEndpoint();
      for (String host : hosts) {
        if (hostMap.containsKey(host)) {
          hostMap.put(host, hostMap.get(host) + bytes);
        } else {
          hostMap.put(host, bytes);
        }
      }
    }
    HashMap<DrillbitEndpoint,Long> ebs = new HashMap();
    try {
      for (Map.Entry<String,Long> hostEntry : hostMap.entrySet()) {
        String host = hostEntry.getKey();
        Long bytes = hostEntry.getValue();
        DrillbitEndpoint d = getDrillBitEndpoint(host);
        if (d != null ) ebs.put(d, bytes);
      }
    } catch (NullPointerException n) {}
    entry.setEndpointBytes(ebs);
  }

  private DrillbitEndpoint getDrillBitEndpoint(String hostName) {
    return endPointMap.get(hostName);
  }

  private void buildEndpointMap() {
    endPointMap = new HashMap<String, DrillbitEndpoint>();
    for (DrillbitEndpoint d : endpoints) {
      String hostName = d.getAddress();
      endPointMap.put(hostName, d);
    }
  }
}
