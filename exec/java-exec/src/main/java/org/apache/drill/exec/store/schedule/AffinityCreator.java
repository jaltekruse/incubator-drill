package org.apache.drill.exec.store.schedule;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.beust.jcommander.internal.Lists;
import com.carrotsearch.hppc.ObjectFloatOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectFloatCursor;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.google.common.base.Stopwatch;

public class AffinityCreator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AffinityCreator.class);
  
  public static <T extends CompleteWork> List<EndpointAffinity> getAffinityMap(List<T> work){
    Stopwatch watch = new Stopwatch();
    
    long totalBytes = 0;
    for (CompleteWork entry : work) {
      totalBytes += entry.getTotalBytes();
    }
    
    ObjectFloatOpenHashMap<DrillbitEndpoint> affinities = new ObjectFloatOpenHashMap<DrillbitEndpoint>();
    for (CompleteWork entry : work) {
      for (ObjectLongCursor<DrillbitEndpoint> cursor : entry.getByteMap()) {
        long bytes = cursor.value;
        float affinity = (float)bytes / (float)totalBytes;
        logger.debug("Work: {} Endpoint: {} Bytes: {}", work, cursor.key.getAddress(), bytes);
        affinities.putOrAdd(cursor.key, affinity, affinity);
      }
    }
    
    List<EndpointAffinity> affinityList = Lists.newLinkedList();
    for (ObjectFloatCursor<DrillbitEndpoint> d : affinities) {
      logger.debug("Endpoint {} has affinity {}", d.key.getAddress(), d.value);
      affinityList.add(new EndpointAffinity(d.key, d.value));
    }

    logger.debug("Took {} ms to get operator affinity", watch.stop().elapsed(TimeUnit.MILLISECONDS));
    return affinityList;
  }
}
