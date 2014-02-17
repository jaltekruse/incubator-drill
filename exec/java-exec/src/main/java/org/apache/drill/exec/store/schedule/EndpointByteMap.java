package org.apache.drill.exec.store.schedule;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;

/**
 * Presents an interface that describes the number of bytes for a particular work unit associated with a particular DrillbitEndpoint.
 */
public interface EndpointByteMap extends Iterable<ObjectLongCursor<DrillbitEndpoint>>{

  public boolean isSet(DrillbitEndpoint endpoint);
  public long get(DrillbitEndpoint endpoint);
  public boolean isEmpty();
  public long getMaxBytes();
  public void add(DrillbitEndpoint endpoint, long bytes);
}