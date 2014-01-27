package org.apache.drill.optiq;

import org.apache.drill.common.expression.FunctionRegistry;

public class DrillParseContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillParseContext.class);
  
  private final FunctionRegistry registry;
  
  public DrillParseContext(FunctionRegistry registry) {
    super();
    this.registry = registry;
  }

  public FunctionRegistry getRegistry(){
    return registry;
  }
  
}
