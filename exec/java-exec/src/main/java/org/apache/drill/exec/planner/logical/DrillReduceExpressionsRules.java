/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.exec.planner.logical;

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.rules.ReduceExpressionsRule;

import java.math.BigDecimal;

public class DrillReduceExpressionsRules {

  public static final DrillReduceFilterRule FILTER_INSTANCE_DRILL =
      new DrillReduceFilterRule();

  public static final DrillReduceFilterRule CALC_INSTANCE_DRILL =
      new DrillReduceFilterRule();

  private static class DrillReduceFilterRule extends ReduceExpressionsRule.ReduceFilterRule {

    DrillReduceFilterRule() {
      super("DrillReduceExpressionsRule[Filter]");
    }

    @Override
    protected RelNode createEmptyRelOrEquivalent(FilterRel filter) {
      return new SortRel(filter.getCluster(), filter.getTraitSet(),
          filter.getChild(),
          RelCollationImpl.EMPTY,
          filter.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)),
          filter.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)));
    }

  }

  private static class DrillReduceCalcRule extends ReduceExpressionsRule.ReduceCalcRule {

    DrillReduceCalcRule() {
      super("DrillReduceExpressionsRule[Filter]");
    }

    @Override
    protected RelNode createEmptyRelOrEquivalent(CalcRel calc) {
      return new SortRel(calc.getCluster(), calc.getTraitSet(),
          calc.getChild(),
          RelCollationImpl.EMPTY,
          calc.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)),
          calc.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)));
    }

  }
}
