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

import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.NlsString;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.List;

public class DrillConstExecutor implements RelOptPlanner.Executor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConstExecutor.class);

  FunctionImplementationRegistry funcImplReg;
  UdfUtilities udfUtilities;
  BufferAllocator allocator;

  public DrillConstExecutor (FunctionImplementationRegistry funcImplReg, BufferAllocator allocator, UdfUtilities udfUtilities) {
    this.funcImplReg = funcImplReg;
    this.udfUtilities = udfUtilities;
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
      InterpreterEvaluator.evaluateConstantExpr(vector, udfUtilities, materializedExpr);

      // TODO - add a switch here to translate expression results to the appropriate literal type
      try {
        switch(materializedExpr.getMajorType().getMinorType()) {
          case INT:
            reducedValues.add(rexBuilder.makeExactLiteral(new BigDecimal((Integer)vector.getAccessor().getObject(0))));
            break;
          case BIGINT:
            reducedValues.add(rexBuilder.makeExactLiteral(new BigDecimal((Long)vector.getAccessor().getObject(0))));
            break;
          case FLOAT4:
            reducedValues.add(rexBuilder.makeApproxLiteral(new BigDecimal((Float)vector.getAccessor().getObject(0))));
            break;
          case FLOAT8:
            reducedValues.add(rexBuilder.makeApproxLiteral(new BigDecimal((Double)vector.getAccessor().getObject(0))));
            break;
          case VARCHAR:
            reducedValues.add(rexBuilder.makeCharLiteral(new NlsString(new String(((VarCharVector) vector).getAccessor().get(0), "UTF-8"), null, null)));
            break;
          case BIT:
            reducedValues.add(rexBuilder.makeLiteral(((BitVector) vector).getAccessor().get(0) == 1 ? true : false));
            break;
        }
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Invalid string returned from constant expression evaluation");
      }
      vector.clear();
    }
  }
}
