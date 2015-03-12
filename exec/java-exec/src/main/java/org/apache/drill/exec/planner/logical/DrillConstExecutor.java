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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import net.hydromatic.avatica.ByteString;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterEvaluator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.IntervalDayVector;
import org.apache.drill.exec.vector.IntervalYearVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.TimeVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
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
import org.joda.time.Period;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;

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
  BufferAllocator allocator;

  public DrillConstExecutor (FunctionImplementationRegistry funcImplReg, BufferAllocator allocator, UdfUtilities udfUtilities) {
    this.funcImplReg = funcImplReg;
    this.udfUtilities = udfUtilities;
    this.allocator = allocator;
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

      final MaterializedField outputField = MaterializedField.create("outCol", materializedExpr.getMajorType());
      ValueVector vector = TypeHelper.getNewVector(outputField, allocator);
      vector.allocateNewSafe();
      InterpreterEvaluator.evaluateConstantExpr(vector, udfUtilities, materializedExpr);
      RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

        switch(materializedExpr.getMajorType().getMinorType()) {
          case INT:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal((Integer)vector.getAccessor().getObject(0)),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTEGER, newCall),
                false));
            break;
          case BIGINT:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal((Long)vector.getAccessor().getObject(0)),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.BIGINT, newCall),
                false));
            break;
          case FLOAT4:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal((Float) vector.getAccessor().getObject(0)),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.FLOAT, newCall),
                false));
            break;
          case FLOAT8:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal((Double) vector.getAccessor().getObject(0)),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.DOUBLE, newCall),
                false));
            break;
          case VARCHAR:
            reducedValues.add(rexBuilder.makeLiteral(
                new NlsString(new String(((VarCharVector) vector).getAccessor().get(0), Charsets.UTF_8), null, null),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.VARCHAR, newCall),
                false));
            break;
          case BIT:
            reducedValues.add(rexBuilder.makeLiteral(
                ((BitVector) vector).getAccessor().get(0) == 1 ? true : false,
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.BOOLEAN, newCall),
                false));
            break;
          case DATE:
            reducedValues.add(rexBuilder.makeLiteral(
                new DateTime(((DateVector)vector).getAccessor().get(0), DateTimeZone.UTC).toCalendar(null),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.DATE, newCall),
                false));
            break;

          case DECIMAL9:
          case DECIMAL18:
          case DECIMAL28SPARSE:
          case DECIMAL38SPARSE:
            reducedValues.add(rexBuilder.makeLiteral(
                vector.getAccessor().getObject(0),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.DECIMAL, newCall),
                false));
            break;

          case TIME:
            reducedValues.add(rexBuilder.makeLiteral(
                new DateTime(((TimeVector) vector).getAccessor().get(0), DateTimeZone.UTC).toCalendar(null),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.TIME, newCall),
                false));
            break;
          case TIMESTAMP:
            reducedValues.add(rexBuilder.makeLiteral(
                new DateTime(((TimeStampVector)vector).getAccessor().get(0), DateTimeZone.UTC).toCalendar(null),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.TIMESTAMP, newCall),
                false));
            break;

          case VARBINARY:
            reducedValues.add(rexBuilder.makeLiteral(
                new ByteString((byte[]) vector.getAccessor().getObject(0)),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.VARBINARY, newCall),
                false));
            break;

          case INTERVALYEAR:
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(((IntervalYearVector)vector).getAccessor().get(0)),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTERVAL_YEAR_MONTH, newCall),
                false));
            break;
          case INTERVALDAY:
            Period p = ((IntervalDayVector)vector).getAccessor().getObject(0);
            reducedValues.add(rexBuilder.makeLiteral(
                new BigDecimal(p.getDays() * DateUtility.daysToStandardMillis + p.getMillis()),
                createCalciteTypeWithNullability(typeFactory, SqlTypeName.INTERVAL_DAY_TIME, newCall),
                false));
            break;


          // TODO - map and list are used in Drill but currently not expressible as literals, these can however be
          // outputs of functions that take literals as inputs (such as a convert_fromJSON with a literal string
          // as input), so we need to identify functions with these return types as non-foldable until we have a
          // literal representation for them
          case MAP:
          case LIST:
            // fall through for now

          // TODO - Is this interval type supported?
          case INTERVAL:
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
      vector.clear();
    }
  }
}


