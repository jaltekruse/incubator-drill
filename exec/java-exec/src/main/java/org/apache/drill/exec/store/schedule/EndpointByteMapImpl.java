package org.apache.drill.exec.store.schedule;

import java.util.Iterator;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;

public class EndpointByteMapImpl implements EndpointByteMap{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EndpointByteMapImpl.class);
  
  private final ObjectLongOpenHashMap<DrillbitEndpoint> map = new ObjectLongOpenHashMap<>();
  
  private long maxBytes;
  
  public boolean isSet(DrillbitEndpoint endpoint){
    return map.containsKey(endpoint);
  }
  
  public long get(DrillbitEndpoint endpoint){
    return map.get(endpoint);
  }
 
  public boolean isEmpty(){
    return map.isEmpty();
  }
  
  public void add(DrillbitEndpoint endpoint, long bytes){
    maxBytes = Math.max(maxBytes, map.putOrAdd(endpoint, bytes, bytes)+1);
  }

  public long getMaxBytes() {
    return maxBytes;
  }

  @Override
  public Iterator<ObjectLongCursor<DrillbitEndpoint>> iterator() {
    return map.iterator();
  }
  
  
}
