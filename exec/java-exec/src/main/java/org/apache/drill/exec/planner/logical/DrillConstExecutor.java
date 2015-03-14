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

import com.google.common.collect.ImmutableList;
import net.hydromatic.avatica.ByteString;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.IntervalDayHolder;
import org.apache.drill.exec.expr.holders.IntervalYearHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.UdfUtilities;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlIntervalQualifier;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.NlsString;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

public class DrillConstExecutor implements RelOptPlanner.Executor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConstExecutor.class);


  // This is a list of all types that cannot be folded at planning time for various reasons, most of the types are
  // currently not supported at all. The reasons for the others can be found in the evaluation code in the reduce method
  public static final List<Object> NON_REDUCIBLE_TYPES =
      ImmutableList.builder().add(TypeProtos.MinorType.INTERVAL, TypeProtos.MinorType.MAP,
                                  TypeProtos.MinorType.LIST, TypeProtos.MinorType.TIMESTAMPTZ, TypeProtos.MinorType.TIMETZ, TypeProtos.MinorType.LATE,
                                  TypeProtos.MinorType.TINYINT, TypeProtos.MinorType.SMALLINT, TypeProtos.MinorType.GENERIC_OBJECT, TypeProtos.MinorType.NULL,
                                  TypeProtos.MinorType.DECIMAL28DENSE, TypeProtos.MinorType.DECIMAL38DENSE, TypeProtos.MinorType.MONEY, TypeProtos.MinorType.VARBINARY,
                                  TypeProtos.MinorType.FIXEDBINARY, TypeProtos.MinorType.FIXEDCHAR, TypeProtos.MinorType.FIXED16CHAR,
                                  TypeProtos.MinorType.VAR16CHAR, TypeProtos.MinorType.UINT1, TypeProtos.MinorType.UINT2, TypeProtos.MinorType.UINT4,
                                  TypeProtos.MinorType.UINT8, TypeProtos.MinorType.DECIMAL9, TypeProtos.MinorType.DECIMAL18,
                                  TypeProtos.MinorType.DECIMAL28SPARSE, TypeProtos.MinorType.DECIMAL38SPARSE).build();

  FunctionImplementationRegistry funcImplReg;
  UdfUtilities udfUtilities;

  public DrillConstExecutor(FunctionImplementationRegistry funcImplReg, UdfUtilities udfUtilities) {
    this.funcImplReg = funcImplReg;
    this.udfUtilities = udfUtilities;
  }

  private RelDataType createCalciteTypeWithNullability(RelDataTypeFactory typeFactory, SqlTypeName sqlTypeName, RexNode node) {
    RelDataType type;
    if (sqlTypeName == SqlTypeName.INTERVAL_DAY_TIME) {
      type = typeFactory.createSqlIntervalType(
          new SqlIntervalQualifier(
              SqlIntervalQualifier.TimeUnit.DAY,
              SqlIntervalQualifier.TimeUnit.MINUTE,
              SqlParserPos.ZERO));
    } else if (sqlTypeName == SqlTypeName.INTERVAL_YEAR_MONTH) {
      type = typeFactory.createSqlIntervalType(
          new SqlIntervalQualifier(
              SqlIntervalQualifier.TimeUnit.YEAR,
              SqlIntervalQualifier.TimeUnit.MONTH,
             SqlParserPos.ZERO));
    } else if (sqlTypeName == SqlTypeName.VARCHAR) {
      type = typeFactory.createSqlType(sqlTypeName, 65536);
    } else {
      type = typeFactory.createSqlType(sqlTypeName);
    }
    return typeFactory.createTypeWithNullability(
        type,
        node.getType().isNullable());
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

      ValueHolder output = InterpreterEvaluator.evaluateConstantExpr(udfUtilities, materializedExpr);
      RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        switch(materializedExpr.getMajorType().getMinorType()) {
          case INT:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((IntHolder)output).value),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTEGER, newCall),
                false));
            break;
          case BIGINT:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((BigIntHolder)output).value),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.BIGINT, newCall),
                false));
            break;
          case FLOAT4:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((Float4Holder)output).value),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.FLOAT, newCall),
                false));
            break;
          case FLOAT8:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((Float8Holder)output).value),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.DOUBLE, newCall),
                false));
            break;
          case VARCHAR:
            reducedValues.add(rexBuilder.makeCharLiteral(
                new NlsString(StringFunctionHelpers.getStringFromVarCharHolder((VarCharHolder)output), null, null)));
            break;
          case BIT:
            reducedValues.add(rexBuilder.makeLiteral(
                ((BitHolder)output).value == 1 ? true : false,
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.BOOLEAN, newCall),
                false));
            break;
          case DATE:
            reducedValues.add(rexBuilder.makeLiteral(
                new DateTime(((DateHolder) output).value, DateTimeZone.UTC).toCalendar(null),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.DATE, newCall),
                false));
            break;
          case DECIMAL9:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(BigInteger.valueOf(((Decimal9Holder) output).value), ((Decimal9Holder)output).scale),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall),
                false));
            break;
          case DECIMAL18:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(BigInteger.valueOf(((Decimal9Holder) output).value), ((Decimal18Holder)output).scale),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall),
                false));
            break;
          case DECIMAL28SPARSE:
            Decimal28SparseHolder decimal28Out = (Decimal28SparseHolder)output;
            reducedValues.add(rexBuilder.makeLiteral(
                org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(
                    decimal28Out.buffer,
                    decimal28Out.start * 20,
                    5,
                    decimal28Out.scale),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall),
                false
            ));
            break;
          case DECIMAL38SPARSE:
            Decimal38SparseHolder decimal38Out = (Decimal38SparseHolder)output;
            reducedValues.add(rexBuilder.makeLiteral(
                org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromSparse(
                    decimal38Out.buffer,
                    decimal38Out.start * 24,
                    6,
                    decimal38Out.scale),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall),
                false));
            break;

          case TIME:
            reducedValues.add(rexBuilder.makeLiteral(
                new DateTime(((TimeHolder)output).value, DateTimeZone.UTC).toCalendar(null),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.TIME, newCall),
                false));
            break;
          case TIMESTAMP:
            reducedValues.add(rexBuilder.makeLiteral(
                new DateTime(((TimeStampHolder)output).value, DateTimeZone.UTC).toCalendar(null),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.TIMESTAMP, newCall),
                false));
            break;

          case VARBINARY:
            VarBinaryHolder varBinOut = (VarBinaryHolder)output;
            final byte[] temp = new byte[varBinOut.end - varBinOut.start];
            varBinOut.buffer.readerIndex(varBinOut.start);
            varBinOut.buffer.readBytes(temp, 0, varBinOut.end - varBinOut.start);
            reducedValues.add(rexBuilder.makeLiteral(
                new ByteString(temp),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.VARBINARY, newCall),
                false));
            break;

          case INTERVALYEAR:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((IntervalYearHolder)output).value),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTERVAL_YEAR_MONTH, newCall),
                false));
            break;
          case INTERVALDAY:
            IntervalDayHolder intervalDayOut = (IntervalDayHolder) output;
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(intervalDayOut.days * DateUtility.daysToStandardMillis + intervalDayOut.milliseconds),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTERVAL_DAY_TIME, newCall),
                false));
            break;
          case INTERVAL:
            // cannot represent this as a literal according to calcite, add the original expression back
            reducedValues.add(newCall);
            break;

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
            reducedValues.add(newCall);
            break;
          default:
            reducedValues.add(newCall);
            break;
        }
    }
  }
}


