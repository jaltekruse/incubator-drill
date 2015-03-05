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
package org.apache.drill.exec.expr.fn.interpreter;

import com.google.common.base.Preconditions;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.NullExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedNullConstant;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.fn.DrillSimpleFuncHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.ops.QueryDateTimeInfo;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.vector.ValueHolderHelper;
import org.apache.drill.exec.vector.ValueVector;

import javax.inject.Inject;
import java.lang.reflect.Field;


public class InterpreterEvaluator {

  public static void evaluateConstantExpr(ValueVector outVV, UdfUtilities udfUtilities, LogicalExpression expr) {
    evaluate(1, udfUtilities, null, outVV, expr);
  }

  public static void evaluate(RecordBatch incoming, ValueVector outVV, LogicalExpression expr) {
    evaluate(incoming.getRecordCount(), incoming.getContext(), incoming, outVV, expr);
  }

  public static void evaluate(int recordCount, UdfUtilities udfUtilities, VectorAccessible incoming, ValueVector outVV, LogicalExpression expr) {

    InitVisitor initVisitor = new InitVisitor(udfUtilities);
    EvalVisitor evalVisitor = new EvalVisitor(incoming, udfUtilities);

    expr.accept(initVisitor, incoming);

    for (int i = 0; i < recordCount; i++) {
      ValueHolder out = expr.accept(evalVisitor, i);
      TypeHelper.setValueSafe(outVV, i, out);
    }

    outVV.getMutator().setValueCount(recordCount);

  }

  private static class InitVisitor extends AbstractExprVisitor<LogicalExpression, VectorAccessible, RuntimeException> {

    private UdfUtilities udfUtilities;
    private LiteralValueMaterializer literalValueMaterializer;

    protected InitVisitor(UdfUtilities udfUtilities) {
      super();
      this.udfUtilities = udfUtilities;
      this.literalValueMaterializer = new LiteralValueMaterializer(udfUtilities);
    }

    @Override
    public LogicalExpression visitFunctionHolderExpression(FunctionHolderExpression holderExpr, VectorAccessible incoming) {
      if (! (holderExpr.getHolder() instanceof DrillSimpleFuncHolder)) {
        throw new UnsupportedOperationException("Only Drill simple UDF can be used in interpreter mode!");
      }

      DrillSimpleFuncHolder holder = (DrillSimpleFuncHolder) holderExpr.getHolder();

      for (int i = 0; i < holderExpr.args.size(); i++) {
        holderExpr.args.get(i).accept(this, incoming);
      }

      ValueHolder[] args = evaluateArguments(holderExpr, literalValueMaterializer);

      try {
        DrillSimpleFunc interpreter = holder.createInterpreter();
        Field[] fields = interpreter.getClass().getDeclaredFields();
        for (Field f : fields) {
          if ( f.getAnnotation(Inject.class) != null ) {
            f.setAccessible(true);
            if (f.getType().equals(DrillBuf.class)) {
              DrillBuf buf = udfUtilities.getManagedBuffer();
              f.set(interpreter, buf);
            } else if (f.getType().equals(QueryDateTimeInfo.class)) {
              f.set(interpreter, udfUtilities.getQueryDateTimeInfo());
            } else {
              // do nothing with the field
            }
          } else { // do nothing with non-inject fields here
            continue;
          }
        }

        setParametersAndMaterializeOutput(interpreter, args, holderExpr.getName());

        interpreter.setup();

        ((DrillFuncHolderExpr) holderExpr).setInterpreter(interpreter);

        return holderExpr;

      } catch (Exception ex) {
        throw new RuntimeException("Error in evaluating function of " + holderExpr.getName() + ": ", ex);
      }
    }

    @Override
    public LogicalExpression visitUnknown(LogicalExpression e, VectorAccessible incoming) throws RuntimeException {
      for (LogicalExpression child : e) {
        child.accept(this, incoming);
      }

      return e;
    }
  }

  private static ValueHolder handleNullResolution(DrillSimpleFuncHolder holder, int argIndex, ValueHolder argument) {
    // In case function use "NULL_IF_NULL" policy.
    if (holder.getNullHandling() == FunctionTemplate.NullHandling.NULL_IF_NULL) {
      // Case 1: parameter is non-nullable, argument is nullable.
      if (holder.getParameters()[argIndex].getType().getMode() == TypeProtos.DataMode.REQUIRED && TypeHelper.getValueHolderType(argument).getMode() == TypeProtos.DataMode.OPTIONAL) {
        // Case 1.1 : argument is null, return null value holder directly.
        if (TypeHelper.isNull(argument)) {
          return TypeHelper.createValueHolder(holder.getReturnType());
        } else {
          // Case 1.2: argument is nullable but not null value, deNullify it.
          return TypeHelper.deNullify(argument);
        }
      } else if (holder.getParameters()[argIndex].getType().getMode() == TypeProtos.DataMode.OPTIONAL && TypeHelper.getValueHolderType(argument).getMode() == TypeProtos.DataMode.REQUIRED) {
        // Case 2: parameter is nullable, argument is non-nullable. Nullify it.
        return TypeHelper.nullify(argument);
      }
    }
    return argument;
  }

  /**
   * Evaluate the input expressions to a DrillSimpleFunc.
   *
   *
   * @param holderExpr -
   * @param constantsOnly
   * @param inIndex
   * @param evalVisitor
   * @return
   */
  private static ValueHolder[] evaluateArguments(FunctionHolderExpression holderExpr,
                                                 boolean constantsOnly,
                                                 int inIndex,
                                                 AbstractExprVisitor<ValueHolder, Integer, RuntimeException> evalVisitor) {

    DrillSimpleFuncHolder holder = (DrillSimpleFuncHolder) holderExpr.getHolder();
    ValueHolder [] args = new ValueHolder [holderExpr.args.size()];
    for (int i = 0; i < holderExpr.args.size(); i++) {
//      if (!constantsOnly || (constantsOnly && holderExpr.args.get(i) instanceof ValueExpressions)) {
      ValueHolder result = holderExpr.args.get(i).accept(evalVisitor, inIndex);
      if (result != null && constantsOnly) {
        args[i] = result;
        args[i] = handleNullResolution(holder, i, args[i]);
      } else if (!constantsOnly) {
        args[i] = holderExpr.args.get(i).accept(evalVisitor, inIndex);
        args[i] = handleNullResolution(holder, i, args[i]);
      }
    }
    return args;
  }


  private static ValueHolder[] evaluateArguments(FunctionHolderExpression holderExpr, LiteralValueMaterializer literalValueMaterializer) {
    return evaluateArguments( holderExpr,
                              true,
                              0 /* unused with constant evaluation */,
                              literalValueMaterializer);
  }

  /**
   *
   * @return - a reference to the output ValueHolder
   */
  private static Field setParametersAndMaterializeOutput(DrillSimpleFunc interpreter, ValueHolder[] args, String functionName) throws InstantiationException {

    // the current input index to assign into the next available parameter, found using the @Param notation
    // the order parameters are declared in the java class for the DrillFunc is meaningful
    int currParameterIndex = 0;
    Field outField = null;
    try {
      Field[] fields = interpreter.getClass().getDeclaredFields();
      for (Field f : fields) {
        // if this is annotated as a parameter to the function
        if ( f.getAnnotation(Param.class) != null ) {
          f.setAccessible(true);
          if (currParameterIndex < args.length) {
            f.set(interpreter, args[currParameterIndex]);
          }
          currParameterIndex++;
        } else if ( f.getAnnotation(Output.class) != null ) {
          if (outField != null) {
            throw new DrillRuntimeException("Malformed DrillFunction with two return values: " + functionName);
          }
          f.setAccessible(true);
          outField = f;
          // create an instance of the holder for the output to be stored in
          f.set(interpreter, f.getType().newInstance());
        }
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    if (args.length != currParameterIndex ) {
      throw new DrillRuntimeException(
          String.format("Wrong number of parameters provided to interpreted expression evaluation " +
              "for function %s, expected %d parameters, but received %d.",
              functionName, currParameterIndex, args.length));
    }
    if (outField == null) {
      throw new DrillRuntimeException("Malformed DrillFunction without a return type: " + functionName);
    }
    return outField;
  }

  public static class EvalVisitor extends AbstractExprVisitor<ValueHolder, Integer, RuntimeException> {
    private VectorAccessible incoming;
    private UdfUtilities udfUtilities;
    private LiteralValueMaterializer literalValueMaterializer;

    protected EvalVisitor(VectorAccessible incoming, UdfUtilities udfUtilities) {
      super();
      this.incoming = incoming;
      this.udfUtilities = udfUtilities;
      this.literalValueMaterializer = new LiteralValueMaterializer(udfUtilities);
    }

    @Override
    public ValueHolder visitFunctionCall(FunctionCall call, Integer value) throws RuntimeException {
      return visitUnknown(call, value);
    }

    @Override
    public ValueHolder visitSchemaPath(SchemaPath path,Integer value) throws RuntimeException {
      return visitUnknown(path, value);
    }

    // TODO - review what to do with these
    // **********************************
    @Override
    public ValueHolder visitCastExpression(CastExpression e,Integer value) throws RuntimeException {
      return visitUnknown(e, value);
    }

    @Override
    public ValueHolder visitConvertExpression(ConvertExpression e,Integer value) throws RuntimeException {
      return visitUnknown(e, value);
    }

    @Override
    public ValueHolder visitNullExpression(NullExpression e,Integer value) throws RuntimeException {
      return visitUnknown(e, value);
    }
    // TODO - review what to do with these (3 functions above)
    //********************************************

    @Override
    public ValueHolder visitFunctionHolderExpression(FunctionHolderExpression holderExpr, Integer inIndex) {
      if (! (holderExpr.getHolder() instanceof DrillSimpleFuncHolder)) {
        throw new UnsupportedOperationException("Only Drill simple UDF can be used in interpreter mode!");
      }


      // add call to new method here
      ValueHolder[] args = evaluateArguments(holderExpr, false, inIndex, this);

      try {
        DrillSimpleFunc interpreter =  ((DrillFuncHolderExpr) holderExpr).getInterpreter();

        Preconditions.checkArgument(interpreter != null, "interpreter could not be null when use interpreted model to evaluate function " + holderExpr.getName());
        Field outField = setParametersAndMaterializeOutput(interpreter, args, holderExpr.getName());

        interpreter.eval();
        ValueHolder out = (ValueHolder) outField.get(interpreter);

        if (TypeHelper.getValueHolderType(out).getMode() == TypeProtos.DataMode.OPTIONAL &&
            holderExpr.getMajorType().getMode() == TypeProtos.DataMode.REQUIRED) {
          return TypeHelper.deNullify(out);
        } else if (TypeHelper.getValueHolderType(out).getMode() == TypeProtos.DataMode.REQUIRED &&
              holderExpr.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL) {
          return TypeHelper.nullify(out);
        } else {
          return out;
        }

      } catch (Exception ex) {
        throw new RuntimeException("Error in evaluating function of " + holderExpr.getName(), ex);
      }

    }

    @Override
    public ValueHolder visitBooleanOperator(BooleanOperator op, Integer inIndex) {
      // Apply short circuit evaluation to boolean operator.
      if (op.getName().equals("booleanAnd")) {
        return visitBooleanAnd(op, inIndex);
      }else if(op.getName().equals("booleanOr")) {
        return visitBooleanOr(op, inIndex);
      } else {
        throw new UnsupportedOperationException("BooleanOperator can only be booleanAnd, booleanOr. You are using " + op.getName());
      }
    }

    @Override
    public ValueHolder visitIfExpression(IfExpression ifExpr, Integer inIndex) throws RuntimeException {
      ValueHolder condHolder = ifExpr.ifCondition.condition.accept(this, inIndex);

      assert (condHolder instanceof BitHolder || condHolder instanceof NullableBitHolder);

      Trivalent flag = isBitOn(condHolder);

      switch (flag) {
        case TRUE:
          return ifExpr.ifCondition.expression.accept(this, inIndex);
        case FALSE:
        case NULL:
          return ifExpr.elseExpression.accept(this, inIndex);
        default:
          throw new UnsupportedOperationException("No other possible choice. Something is not right");
      }
    }

    @Override
    public ValueHolder visitUnknown(LogicalExpression e, Integer inIndex) throws RuntimeException {
      if (e instanceof ValueVectorReadExpression) {
        return visitValueVectorReadExpression((ValueVectorReadExpression) e, inIndex);
      } else {
        ValueHolder result = e.accept(literalValueMaterializer, inIndex);
        if (result != null) {
          return result;
        }
        return super.visitUnknown(e, inIndex);
      }

    }

    protected ValueHolder visitValueVectorReadExpression(ValueVectorReadExpression e, Integer inIndex)
        throws RuntimeException {
      TypeProtos.MajorType type = e.getMajorType();

      ValueVector vv;
      ValueHolder holder;
      try {
        switch (type.getMode()) {
          case OPTIONAL:
          case REQUIRED:
            vv = incoming.getValueAccessorById(TypeHelper.getValueVectorClass(type.getMinorType(),type.getMode()), e.getFieldId().getFieldIds()).getValueVector();
            holder = TypeHelper.getValue(vv, inIndex.intValue());
            return holder;
          default:
            throw new UnsupportedOperationException("Type of " + type + " is not supported yet in interpreted expression evaluation!");
        }
      } catch (Exception ex){
        throw new DrillRuntimeException("Error when evaluate a ValueVectorReadExpression: " + ex);
      }
    }

    // Use Kleene algebra for three-valued logic :
    //  value of boolean "and" when one side is null
    //    p       q     p and q
    //    true    null     null
    //    false   null     false
    //    null    true     null
    //    null    false    false
    //    null    null     null
    //  "and" : 1) if any argument is false, return false. false is earlyExitValue.
    //          2) if none argument is false, but at least one is null, return null.
    //          3) finally, return true (finalValue).
    private ValueHolder visitBooleanAnd(BooleanOperator op, Integer inIndex) {
      ValueHolder [] args = new ValueHolder [op.args.size()];
      boolean hasNull = false;
      ValueHolder out = null;
      for (int i = 0; i < op.args.size(); i++) {
        args[i] = op.args.get(i).accept(this, inIndex);

        Trivalent flag = isBitOn(args[i]);

        switch (flag) {
          case FALSE:
            return op.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL? TypeHelper.nullify(ValueHolderHelper.getBitHolder(0)) : ValueHolderHelper.getBitHolder(0);
          case NULL:
            hasNull = true;
          case TRUE:
        }
      }

      if (hasNull) {
        return ValueHolderHelper.getNullableBitHolder(true, 0);
      } else {
        return op.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL? TypeHelper.nullify(ValueHolderHelper.getBitHolder(1)) : ValueHolderHelper.getBitHolder(1);
      }
    }

    //  value of boolean "or" when one side is null
    //    p       q       p and q
    //    true    null     true
    //    false   null     null
    //    null    true     true
    //    null    false    null
    //    null    null     null
    private ValueHolder visitBooleanOr(BooleanOperator op, Integer inIndex) {
      ValueHolder [] args = new ValueHolder [op.args.size()];
      boolean hasNull = false;
      for (int i = 0; i < op.args.size(); i++) {
        args[i] = op.args.get(i).accept(this, inIndex);

        Trivalent flag = isBitOn(args[i]);

        switch (flag) {
          case TRUE:
            return op.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL? TypeHelper.nullify(ValueHolderHelper.getBitHolder(1)) : ValueHolderHelper.getBitHolder(1);
          case NULL:
            hasNull = true;
          case FALSE:
        }
      }

      if (hasNull) {
        return ValueHolderHelper.getNullableBitHolder(true, 0);
      } else {
        return op.getMajorType().getMode() == TypeProtos.DataMode.OPTIONAL? TypeHelper.nullify(ValueHolderHelper.getBitHolder(0)) : ValueHolderHelper.getBitHolder(0);
      }
    }

    public enum Trivalent {
      FALSE,
      TRUE,
      NULL
    }

    private Trivalent isBitOn(ValueHolder holder) {
      assert (holder instanceof BitHolder || holder instanceof NullableBitHolder);

      if ( (holder instanceof BitHolder && ((BitHolder) holder).value == 1)) {
        return Trivalent.TRUE;
      } else if (holder instanceof NullableBitHolder && ((NullableBitHolder) holder).isSet == 1 && ((NullableBitHolder) holder).value == 1) {
        return Trivalent.TRUE;
      } else if (holder instanceof NullableBitHolder && ((NullableBitHolder) holder).isSet == 0) {
        return Trivalent.NULL;
      } else {
        return Trivalent.FALSE;
      }
    }
  }

  /**
   * Visitor for Drill logical expressions that returns a value only if the expression being visited is a single
   * literal value.
   *
   * This allows for materialization of constant values before the setup method is called, in some cases
   * the constants are used once to materialize a static resource like a regex pattern matcher for the 'like' function.
   *
   * If the logical expression being visited is not a literal, NULL will be returned.
   *
   * Note: Adding new data types will require them to be added to this visitor. Anything that has not been
   * defined here falls back to the superclass behavior of calling visitUnknown(), which has been overridden here
   * to return NULL.
   */
  private static class LiteralValueMaterializer extends AbstractExprVisitor<ValueHolder, Integer /* unused */ , RuntimeException> {

    private UdfUtilities udfUtilities;

    protected LiteralValueMaterializer(UdfUtilities udfUtilities) {
      super();
      this.udfUtilities = udfUtilities;
    }

    public DrillBuf getManagedBufferIfAvailable() {
      return udfUtilities.getManagedBuffer();
    }

    @Override
    public ValueHolder visitDecimal9Constant(ValueExpressions.Decimal9Expression decExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getDecimal9Holder(decExpr.getIntFromDecimal(), decExpr.getScale(), decExpr.getPrecision());
    }

    @Override
    public ValueHolder visitDecimal18Constant(ValueExpressions.Decimal18Expression decExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getDecimal18Holder(decExpr.getLongFromDecimal(), decExpr.getScale(), decExpr.getPrecision());
    }

    @Override
    public ValueHolder visitDecimal28Constant(ValueExpressions.Decimal28Expression decExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getDecimal28Holder(getManagedBufferIfAvailable(), decExpr.getBigDecimal().toString());
    }

    @Override
    public ValueHolder visitDecimal38Constant(ValueExpressions.Decimal38Expression decExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getDecimal28Holder(getManagedBufferIfAvailable(), decExpr.getBigDecimal().toString());
    }

    @Override
    public ValueHolder visitDateConstant(ValueExpressions.DateExpression dateExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getDateHolder(dateExpr.getDate());
    }

    @Override
    public ValueHolder visitTimeConstant(ValueExpressions.TimeExpression timeExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getTimeHolder(timeExpr.getTime());
    }

    @Override
    public ValueHolder visitTimeStampConstant(ValueExpressions.TimeStampExpression timestampExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getTimeStampHolder(timestampExpr.getTimeStamp());
    }

    @Override
    public ValueHolder visitIntervalYearConstant(ValueExpressions.IntervalYearExpression intExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getIntervalYearHolder(intExpr.getIntervalYear());
    }

    @Override
    public ValueHolder visitIntervalDayConstant(ValueExpressions.IntervalDayExpression intExpr,Integer value) throws RuntimeException {
      return ValueHolderHelper.getIntervalDayHolder(intExpr.getIntervalDay(), intExpr.getIntervalMillis());
    }

    @Override
    public ValueHolder visitBooleanConstant(ValueExpressions.BooleanExpression e,Integer value) throws RuntimeException {
      return ValueHolderHelper.getBitHolder(e.getBoolean() == false ? 0 : 1);
    }

    @Override
    public ValueHolder visitNullConstant(TypedNullConstant e,Integer value) throws RuntimeException {
      // create a value holder for the given type, defaults to NULL value if not set
      return TypeHelper.createValueHolder(e.getMajorType());
    }

    @Override
    public ValueHolder visitIntConstant(ValueExpressions.IntExpression e, Integer inIndex) throws RuntimeException {
      return ValueHolderHelper.getIntHolder(e.getInt());
    }

    @Override
    public ValueHolder visitFloatConstant(ValueExpressions.FloatExpression fExpr, Integer value) throws RuntimeException {
      return ValueHolderHelper.getFloat4Holder(fExpr.getFloat());
    }

    @Override
    public ValueHolder visitLongConstant(ValueExpressions.LongExpression intExpr, Integer value) throws RuntimeException {
      return ValueHolderHelper.getBigIntHolder(intExpr.getLong());
    }

    @Override
    public ValueHolder visitDoubleConstant(ValueExpressions.DoubleExpression dExpr, Integer value) throws RuntimeException {
      return ValueHolderHelper.getFloat8Holder(dExpr.getDouble());
    }

    @Override
    public ValueHolder visitQuotedStringConstant(ValueExpressions.QuotedString e, Integer value) throws RuntimeException {
      return ValueHolderHelper.getVarCharHolder(getManagedBufferIfAvailable(), e.value);
    }

    @Override
    public ValueHolder visitUnknown(LogicalExpression e, Integer value) throws RuntimeException {
      // This is intentional, an exception is not used here as this visitor is designed to only materialize
      // literal values, but there is no easy way to identify these expressions before sending them into
      // a visitor. Users of the visitor must guard against this case for all other types of
      // expressions appropriately.
      return null;
    }
  }
}

