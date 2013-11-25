package org.apache.drill.common.logical.data;

import com.google.common.base.Preconditions;

public abstract class AbstractSingleBuilder<T extends SingleInputOperator, X extends AbstractSingleBuilder<T, X>> extends AbstractBuilder<T> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSingleBuilder.class);
  
  private LogicalOperator input;

  public final T build(){
    Preconditions.checkNotNull(input);
    T out = internalBuild();
    out.setInput(input);
    return out;
  }
  
  public X setInput(LogicalOperator input){
    this.input = input;
    return (X) this;
  }
  
  public abstract T internalBuild();

}
