package org.apache.drill.exec.store;


import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.config.ParquetScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class AffinityCalculator {

  BlockLocation[] blocks;
  ImmutableRangeMap<Long,BlockLocation> blockMap;

  public AffinityCalculator(BlockLocation[] blocks) {
    this.blocks = blocks;
    ImmutableRangeMap.Builder<Long, BlockLocation> blockMapBuilder = new ImmutableRangeMap.Builder<Long,BlockLocation>();
    for (BlockLocation block : blocks) {
      long start = block.getOffset();
      long end = start + block.getLength();
      Range<Long> range = Range.closedOpen(start, end);
      blockMapBuilder = blockMapBuilder.put(range, block);
    }
    blockMap = blockMapBuilder.build();
  }

  public AffinityCalculator(String fileName) {
    new AffinityCalculator(new Configuration(), fileName);
  }

  public AffinityCalculator(Configuration conf, String fileName) {
    try {
      FileSystem fs = FileSystem.get(conf);
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

  public void calculate(ParquetScan.ParquetReadEntry entry) {
    HashMap<String,Long> hostMap = null;
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
    HashSet<ParquetScan.EndpointBytes> ebs = new HashSet<>();
    for (Map.Entry<String,Long> hostEntry : hostMap.entrySet()) {
      String host = hostEntry.getKey();
      Long bytes = hostEntry.getValue();
      ebs.add(new ParquetScan.EndpointBytes(getDrillBitEndpoint(host), bytes));
    }
    entry.setEndpointBytes(ebs.toArray(new ParquetScan.EndpointBytes[ebs.size()]));
  }

  public DrillbitEndpoint getDrillBitEndpoint(String hostname) {
    return null;
  }

  public BlockLocation getBlockLocation(long offset) {
    return blockMap.get(offset);
  }
}
