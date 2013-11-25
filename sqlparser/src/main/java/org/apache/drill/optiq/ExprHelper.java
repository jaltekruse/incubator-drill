package org.apache.drill.optiq;

import java.util.List;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;

public class ExprHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExprHelper.class);
  
  private final static String COMPOUND_FAIL_MESSAGE = "The current Optiq based logical plan interpreter does not complicated expressions.  For Order By and Filter";
  
  public static String getAggregateFieldName(FunctionCall c){
    List<LogicalExpression> exprs = c.args;
    if(exprs.size() != 1) throw new UnsupportedOperationException(COMPOUND_FAIL_MESSAGE);
    return getFieldName(exprs.iterator().next());
  }
  
  public static String getFieldName(LogicalExpression e){
    if(e instanceof SchemaPath) return ((SchemaPath) e).getPath().toString();
    throw new UnsupportedOperationException(COMPOUND_FAIL_MESSAGE);
  }
}
