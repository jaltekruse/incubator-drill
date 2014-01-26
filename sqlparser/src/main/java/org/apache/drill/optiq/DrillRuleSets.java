package org.apache.drill.optiq;

import java.util.Iterator;

import net.hydromatic.optiq.tools.RuleSet;

import org.eigenbase.rel.rules.MergeProjectRule;
import org.eigenbase.rel.rules.PushFilterPastJoinRule;
import org.eigenbase.rel.rules.PushFilterPastProjectRule;
import org.eigenbase.rel.rules.PushJoinThroughJoinRule;
import org.eigenbase.rel.rules.PushSortPastProjectRule;
import org.eigenbase.rel.rules.ReduceAggregatesRule;
import org.eigenbase.rel.rules.RemoveDistinctAggregateRule;
import org.eigenbase.rel.rules.RemoveDistinctRule;
import org.eigenbase.rel.rules.RemoveSortRule;
import org.eigenbase.rel.rules.RemoveTrivialCalcRule;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.rel.rules.SwapJoinRule;
import org.eigenbase.rel.rules.TableAccessRule;
import org.eigenbase.rel.rules.UnionToDistinctRule;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.volcano.AbstractConverter.ExpandConversionRule;

import com.google.common.collect.ImmutableSet;

public class DrillRuleSets {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRuleSets.class);

  public static final RuleSet DRILL_BASIC_RULES = new DrillRuleSet(ImmutableSet.of( //

//      ExpandConversionRule.instance,
//      SwapJoinRule.instance,
//      RemoveDistinctRule.instance,
//      UnionToDistinctRule.instance,
//      RemoveTrivialProjectRule.instance,
//      RemoveTrivialCalcRule.instance,
//      RemoveSortRule.INSTANCE,
//
//      TableAccessRule.instance, //
//      MergeProjectRule.instance, //
//      PushFilterPastProjectRule.instance, //
//      PushFilterPastJoinRule.FILTER_ON_JOIN, //
//      RemoveDistinctAggregateRule.instance, //
//      ReduceAggregatesRule.instance, //
//      SwapJoinRule.instance, //
//      PushJoinThroughJoinRule.RIGHT, //
//      PushJoinThroughJoinRule.LEFT, //
//      PushSortPastProjectRule.INSTANCE, //
      
      DrillScanRule.INSTANCE,
      DrillFilterRule.INSTANCE,
      DrillProjectRule.INSTANCE,
      DrillAggregateRule.INSTANCE,

      DrillLimitRule.INSTANCE,
      DrillSortRule.INSTANCE,
      DrillJoinRule.INSTANCE,
      DrillUnionRule.INSTANCE
      ));
  
  
  private static class DrillRuleSet implements RuleSet{
    final ImmutableSet<RelOptRule> rules;

    public DrillRuleSet(ImmutableSet<RelOptRule> rules) {
      super();
      this.rules = rules;
    }

    @Override
    public Iterator<RelOptRule> iterator() {
      return rules.iterator();
    }
  }
}
