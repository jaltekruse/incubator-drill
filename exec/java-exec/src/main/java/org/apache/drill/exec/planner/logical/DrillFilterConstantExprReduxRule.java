/**
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
 */
package org.apache.drill.exec.planner.logical;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.rex.RexShuttle;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.util.NlsString;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Rule that converts a {@link org.eigenbase.rel.FilterRel} to a Drill "filter" operation.
 */
public class DrillFilterConstantExprReduxRule extends RelOptRule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFilterConstantExprReduxRule.class);

  FilterRel filter;
  FunctionImplementationRegistry funcImplReg;
  BufferAllocator allocator;

  DrillFilterConstantExprReduxRule(FunctionImplementationRegistry funcImplReg, BufferAllocator allocator) {
    super(RelOptHelper.any(FilterRel.class, Convention.NONE), "DrillFilterConstantExprReduxRule");
    this.funcImplReg = funcImplReg;
    this.allocator = allocator;
  }

  private DrillFilterConstantExprReduxRule() {
    super(RelOptHelper.any(FilterRel.class, Convention.NONE), "DrillFilterConstantExprReduxRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FilterRel filter = (FilterRel) call.rel(0);
    this.filter = filter;
    filter.getChildExps();
    final RelNode input = filter.getChild();
    //final RelTraitSet traits = filter.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    final RelNode convertedInput = convert(input, input.getTraitSet().plus(DrillRel.DRILL_LOGICAL));
    final RexNode convertedFilterExpr = filter.getCondition().accept(new ConstantExprEvaluator());
    call.transformTo(new DrillFilterRel(filter.getCluster(), convertedInput.getTraitSet(), convertedInput,convertedFilterExpr));
  }

  public static class DrillConstExecutor implements RelOptPlanner.Executor {

    FunctionImplementationRegistry funcImplReg;
    BufferAllocator allocator;

    public DrillConstExecutor (FunctionImplementationRegistry funcImplReg, BufferAllocator allocator) {
      this.funcImplReg = funcImplReg;
      this.allocator = allocator;
    }

    @Override
    public void reduce(RexBuilder rexBuilder, List<RexNode> constExps, List<RexNode> reducedValues) {
      for (RexNode newCall : constExps) {
        LogicalExpression logEx = DrillOptiq.toDrill(new DrillParseContext(), null /* input rel */, newCall);

        ErrorCollectorImpl errors = new ErrorCollectorImpl();
        LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(logEx, null, errors, funcImplReg);
        if (errors.getErrorCount() != 0) {
          logger.error("Failure while materializing expression [{}].  Errors: {}", newCall, errors);
        }
        // TODO - remove this restriction by addressing the TODOs below
          final MaterializedField outputField = MaterializedField.create("outCol", materializedExpr.getMajorType());
          ValueVector vector = TypeHelper.getNewVector(outputField, allocator);
          vector.allocateNewSafe();
          InterpreterEvaluator.evaluateConstantExpr(vector, allocator, materializedExpr);

          // TODO - add a switch here to translate expression results to the appropriate literal type
          try {
            switch(materializedExpr.getMajorType().getMinorType()) {
              case VARCHAR:
                reducedValues.add(rexBuilder.makeCharLiteral(new NlsString(new String(((VarCharVector) vector).getAccessor().get(0), "UTF-8"), null, null)));
                break;
              case BIT:
                reducedValues.add(rexBuilder.makeLiteral(((BitVector) vector).getAccessor().get(0) == 1 ? true : false));
            }
          } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Invalid string returned from constant expression evaluation");
          }
      }
    }
  }

  private class ConstantExprEvaluator extends RexShuttle {

    // this is going to be run on every filter, may need to limit the RexCalls we will traverse the children
    // of to a small subset of expressions. Or possibly create a custom ConstantFinder rexVisitor that will only
    // return true if the immediate children are literatls
    public RexNode visitCall(final RexCall call) {

      RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl());

      boolean exprIsConst = true;
      RexProgram rexProg = RexProgram.createIdentity(filter.getInput(0).getRowType());
      ArrayList<RexNode> newChildren = new ArrayList<>();
      for (RexNode child : call.getOperands()) {
        RexNode newChild = child.accept(this);
        if (!rexProg.isConstant(newChild)) {
          exprIsConst = false;
        }
        newChildren.add(newChild);
      }
      if (exprIsConst) {
        RexNode newCall = rexBuilder.makeCall(call.getOperator(), newChildren);
        LogicalExpression logEx = DrillOptiq.toDrill(new DrillParseContext(), DrillFilterConstantExprReduxRule.this.filter, newCall);

        ErrorCollectorImpl errors = new ErrorCollectorImpl();
        LogicalExpression materializedExpr = ExpressionTreeMaterializer.materialize(logEx, null, errors, funcImplReg);
        if (errors.getErrorCount() != 0) {
          logger.error("Failure while materializing expression [{}].  Errors: {}", newCall, errors);
        }
        // TODO - remove this restriction by addressing the TODOs below
        if (materializedExpr.getMajorType().getMinorType() == TypeProtos.MinorType.VARCHAR) {
          final MaterializedField outputField = MaterializedField.create("outCol", materializedExpr.getMajorType());
          ValueVector vector = TypeHelper.getNewVector(outputField, allocator);
          vector.allocateNewSafe();
          InterpreterEvaluator.evaluateConstantExpr(vector, allocator, materializedExpr);

          try {
            return rexBuilder.makeCharLiteral(new NlsString(new String(((VarCharVector) vector).getAccessor().get(0), "UTF-8"), null, null));
          } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Invalid string returned from constant expression evaluation");
          }
        } else {
          return call;
        }
      } else {
        return rexBuilder.makeCall(call.getOperator(), newChildren);
      }
    }
  }
}