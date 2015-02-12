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

import net.hydromatic.avatica.ByteString;
import org.apache.drill.common.exceptions.DrillRuntimeException;
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
import org.joda.time.DateTime;
import org.joda.time.Period;

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

      final MaterializedField outputField = MaterializedField.create("outCol", materializedExpr.getMajorType());
      ValueVector vector = TypeHelper.getNewVector(outputField, allocator);
      vector.allocateNewSafe();
      InterpreterEvaluator.evaluateConstantExpr(vector, udfUtilities, materializedExpr);

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
          case DECIMAL9:
          case DECIMAL18:
          case DECIMAL28SPARSE:
          case DECIMAL38SPARSE:
            reducedValues.add(rexBuilder.makeExactLiteral((BigDecimal) vector.getAccessor().getObject(0)));
            break;
          case DATE:
            reducedValues.add(rexBuilder.makeDateLiteral(((DateTime)vector.getAccessor().getObject(0)).toCalendar(null)));
            break;
          case TIME:
            // TODO - review the given precision value, chose the maximum available on SQL server
            // https://msdn.microsoft.com/en-us/library/bb677243.aspx
            reducedValues.add(rexBuilder.makeTimeLiteral(((DateTime) vector.getAccessor().getObject(0)).toCalendar(null), 7));
          case TIMESTAMP:
            // TODO - review the given precision value, could not find a good recommendation, reusing value of 7 from time
            reducedValues.add(rexBuilder.makeTimestampLiteral(((DateTime) vector.getAccessor().getObject(0)).toCalendar(null), 7));
            break;
          case VARBINARY:
            reducedValues.add(rexBuilder.makeBinaryLiteral(new ByteString((byte[]) vector.getAccessor().getObject(0))));
            break;

          // TODO - not sure how to populate the SqlIntervalQualifier parameter of the rexBuilder.makeIntervalLiteral method
          // will make these non-reducible at planning time for now
          case INTERVAL:
          case INTERVALYEAR:
          case INTERVALDAY:
            // fall through for now
//            reducedValues.add(rexBuilder.makeIntervalLiteral(((Period) vector.getAccessor().getObject(0)));

          // TODO - map and list are used in Drill but currently not expressible as literals, these can however be
          // outputs of functions that take literals as inputs (such as a convert_fromJSON with a literal string
          // as input), so we need to identify functions with these return types as non-foldable until we have a
          // literal representation for them
          case MAP:
          case LIST:
            // fall through for now

          // currently unsupported types
          case TIMESTAMPTZ:
          case TIMETZ:
          case LATE:
          case TINYINT:
          case SMALLINT:
          case GENERIC_OBJECT:
          case NULL:
          case DECIMAL28DENSE:
          case DECIMAL38DENSE:
          case MONEY:
          case FIXEDBINARY:
          case FIXEDCHAR:
          case FIXED16CHAR:
          case VAR16CHAR:
          case UINT1:
          case UINT2:
          case UINT4:
          case UINT8:
            throw new DrillRuntimeException("Unsupported type returned during planning time constant expression folding: "
                + materializedExpr.getMajorType().getMinorType() );
        }
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Invalid string returned from constant expression evaluation");
      }
      vector.clear();
    }
  }
}
