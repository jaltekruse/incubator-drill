package org.apache.drill.exec.store.schedule;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

public class PartialWork {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartialWork.class);
  
  private final long length;
  private final DrillbitEndpoint[] locations;
  
  public PartialWork(long length, DrillbitEndpoint[] locations) {
    super();
    this.length = length;
    this.locations = locations;
  }
  
  public long getLength() {
    return length;
  }
  public DrillbitEndpoint[] getLocations() {
    return locations;
  }
  
  
  
}
