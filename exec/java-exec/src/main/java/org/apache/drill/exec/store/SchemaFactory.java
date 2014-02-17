package org.apache.drill.exec.store;

import net.hydromatic.optiq.SchemaPlus;

public interface SchemaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaFactory.class);
  
  public void add(SchemaPlus parent);
}
