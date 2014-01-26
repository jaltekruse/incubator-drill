package org.apache.drill.optiq;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelTrait;

public class RelOptHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RelOptHelper.class);
  
  public static RelOptRuleOperand any(Class<? extends RelNode> first, RelTrait trait){
    return RelOptRule.operand(first, trait, RelOptRule.any());
  }

  public static RelOptRuleOperand any(Class<? extends RelNode> first){
    return RelOptRule.operand(first, RelOptRule.any());
  }
  
  public static RelOptRuleOperand some(Class<? extends RelNode> rel, RelOptRuleOperand first, RelOptRuleOperand... rest){
    return RelOptRule.operand(rel, RelOptRule.some(first, rest));
  }

  public static RelOptRuleOperand some(Class<? extends RelNode> rel, RelTrait trait, RelOptRuleOperand first, RelOptRuleOperand... rest){
    return RelOptRule.operand(rel, trait, RelOptRule.some(first, rest));
  }

}
