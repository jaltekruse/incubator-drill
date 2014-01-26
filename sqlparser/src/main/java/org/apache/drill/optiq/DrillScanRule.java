package org.apache.drill.optiq;

import net.hydromatic.optiq.rules.java.JavaRules.EnumerableTableAccessRel;

import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

public class DrillScanRule  extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillScanRule();

  private DrillScanRule() {
    super(RelOptRule.any(EnumerableTableAccessRel.class), "DrillTableRule");
  }




  @Override
  public void onMatch(RelOptRuleCall call) {
    final EnumerableTableAccessRel access = (EnumerableTableAccessRel) call.rel(0);
    final RelTraitSet traits = access.getTraitSet().plus(DrillRel.CONVENTION);
    call.transformTo(new DrillScanRel(access.getCluster(), traits, access.getTable()));
  }
}
