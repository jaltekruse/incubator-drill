package org.apache.drill.common.logical.data;

import java.util.List;

import org.apache.drill.common.expression.LogicalExpression;

public abstract class AbstractBuilder<T extends LogicalOperator> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractBuilder.class);
  
  public abstract T build();
  
  protected LogicalExpression[] aL(List<LogicalExpression> exprs){
    return exprs.toArray(new LogicalExpression[exprs.size()]);
  }

  protected NamedExpression[] aN(List<NamedExpression> exprs){
    return exprs.toArray(new NamedExpression[exprs.size()]);
  }

}
