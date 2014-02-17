package org.apache.drill.exec.store.schedule;


/**
 * Container that holds a complete work unit.  Can contain one or more partial units.
 */
public interface CompleteWork extends Comparable<CompleteWork>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompleteWork.class);
  
  public long getTotalBytes();
  public EndpointByteMap getByteMap();
}
