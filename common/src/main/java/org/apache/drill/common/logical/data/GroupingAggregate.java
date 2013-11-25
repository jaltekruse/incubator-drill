package org.apache.drill.common.logical.data;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

@JsonTypeName("groupingaggregate")
public class GroupingAggregate extends SingleInputOperator{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GroupingAggregate.class);
  
  private final NamedExpression[] keys;
  private final NamedExpression[] exprs;
  
  public GroupingAggregate(@JsonProperty("keys") NamedExpression[] keys, @JsonProperty("exprs") NamedExpression[] exprs) {
    super();
    this.keys = keys;
    this.exprs = exprs;
  }
  
  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
      return logicalVisitor.visitGroupingAggregate(this, value);
  }

  @Override
  public Iterator<LogicalOperator> iterator() {
    return Iterators.singletonIterator(getInput());
  }

  public static Builder builder(){
    return new Builder();
  }
  
  public NamedExpression[] getKeys(){
    return keys;
  }
  
  public NamedExpression[] getExprs(){
    return exprs;
  }
  
  public static class Builder extends AbstractSingleBuilder<GroupingAggregate, Builder>{
    private List<NamedExpression> keys = Lists.newArrayList();
    private List<NamedExpression> exprs = Lists.newArrayList();
    
    public Builder addKey(FieldReference ref, LogicalExpression expr){
      keys.add(new NamedExpression(expr, ref));
      return this;
    }

    public Builder addKey(NamedExpression expr){
      keys.add(expr);
      return this;
    }
    
    public Builder addExpr(NamedExpression expr){
      exprs.add(expr);
      return this;
    }
    
    public Builder addExpr(FieldReference ref, LogicalExpression expr){
      exprs.add(new NamedExpression(expr, ref));
      return this;
    }

    public GroupingAggregate internalBuild(){
      GroupingAggregate ga =  new GroupingAggregate(aN(keys), aN(exprs));
      return ga;
    }
    
  }
  
  
     
}
