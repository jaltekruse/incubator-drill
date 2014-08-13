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
package org.apache.drill.exec.expr.fn.impl.conv;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableDateHolder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.record.RecordBatch;

public class ReadOrNull {

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "read_float4", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class ReadFloat4OrNull implements DrillSimpleFunc{

    @Param VarCharHolder in;
    @Output NullableFloat8Holder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {
      if (in.end - in.start == 0) {
        out.isSet = 0;
      } else {
        byte[] buf = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, buf, 0, in.end - in.start);

        //TODO: need capture format exception, and issue SQLERR code.
        out.value = Float.parseFloat(new String(buf, com.google.common.base.Charsets.UTF_8));
        out.isSet = 1;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "read_float4", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class ReadFloat4OrNullAllowIncomminNulls implements DrillSimpleFunc{

    @Param NullableVarCharHolder in;
    @Output NullableFloat8Holder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {
      if (in.end - in.start == 0) {
        out.isSet = 0;
      } else {
        byte[] buf = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, buf, 0, in.end - in.start);

        //TODO: need capture format exception, and issue SQLERR code.
        out.value = Float.parseFloat(new String(buf, com.google.common.base.Charsets.UTF_8));
        out.isSet = 1;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "read_float8", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class ReadFloat8OrNull implements DrillSimpleFunc{

    @Param VarCharHolder in;
    @Output NullableFloat8Holder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {
      if (in.end - in.start == 0) {
        out.isSet = 0;
      } else {
        byte[] buf = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, buf, 0, in.end - in.start);

        //TODO: need capture format exception, and issue SQLERR code.
        out.value = Double.parseDouble(new String(buf, com.google.common.base.Charsets.UTF_8));
        out.isSet = 1;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "read_float8", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class ReadFloat8OrNullAllowIncommingNulls implements DrillSimpleFunc{

    @Param NullableVarCharHolder in;
    @Output NullableFloat8Holder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {
      if (in.end - in.start == 0) {
        out.isSet = 0;
      } else {
        byte[] buf = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, buf, 0, in.end - in.start);

        //TODO: need capture format exception, and issue SQLERR code.
        out.value = Double.parseDouble(new String(buf, com.google.common.base.Charsets.UTF_8));
        out.isSet = 1;
      }
    }
  }

      @SuppressWarnings("unused")
  @FunctionTemplate(names = {"read_date", "datetype"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL,
      costCategory = FunctionTemplate.FunctionCostCategory.COMPLEX)
  public static class CastVarCharToDate implements DrillSimpleFunc {

    @Param VarCharHolder in;
    @Output NullableDateHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      if (in.end - in.start == 0) {
        out.isSet = 0;
      } else {
        byte[] buf = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
        String input = new String(buf, com.google.common.base.Charsets.UTF_8);

        org.joda.time.format.DateTimeFormatter f = org.apache.drill.exec.expr.fn.impl.DateUtility.getDateTimeFormatter();
        out.value = (org.joda.time.DateMidnight.parse(input, f).withZoneRetainFields(org.joda.time.DateTimeZone.UTC)).getMillis();
        out.isSet = 1;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"read_date", "datetype"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL,
      costCategory = FunctionTemplate.FunctionCostCategory.COMPLEX)
  public static class CastVarCharToDateAllowIncommingNulls  implements DrillSimpleFunc {

    @Param NullableVarCharHolder in;
    @Output NullableDateHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      if (in.end - in.start == 0) {
        out.isSet = 0;
      } else {
        byte[] buf = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
        String input = new String(buf, com.google.common.base.Charsets.UTF_8);

        org.joda.time.format.DateTimeFormatter f = org.apache.drill.exec.expr.fn.impl.DateUtility.getDateTimeFormatter();
        out.value = (org.joda.time.DateMidnight.parse(input, f).withZoneRetainFields(org.joda.time.DateTimeZone.UTC)).getMillis();
        out.isSet = 1;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "read_bigint", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarCharBigInt implements DrillSimpleFunc{

    @Param VarCharHolder in;
    @Output NullableBigIntHolder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {

      if ((in.end - in.start) ==0) {
        //empty, not a valid number
        //byte[] buf = new byte[in.end - in.start];
        //in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
        //throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
        out.isSet = 0;
      } else {
        int readIndex = in.start;

        boolean negative = in.buffer.getByte(readIndex) == '-';

        if (negative && ++readIndex == in.end) {
          //only one single '-'
          byte[] buf = new byte[in.end - in.start];
          in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
          throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
        }

        int radix = 10;
        long max = -Long.MAX_VALUE / radix;
        long result = 0;
        int digit;

        while (readIndex < in.end) {
          digit = Character.digit(in.buffer.getByte(readIndex++),radix);
          //not valid digit.
          if (digit == -1) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
          //overflow
          if (max > result) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }

          long next = result * radix - digit;

          //overflow
          if (next > result) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
          result = next;
        }
        if (!negative) {
          result = -result;
          //overflow
          if (result < 0) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
        }

        out.isSet = 1;
        out.value = result;
      }

    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "read_bigint", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarCharBigIntAllowIncommingNulls implements DrillSimpleFunc{

    @Param NullableVarCharHolder in;
    @Output NullableBigIntHolder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {

      if ((in.end - in.start) ==0) {
        //empty, not a valid number
        //byte[] buf = new byte[in.end - in.start];
        //in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
        //throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
        out.isSet = 0;
      } else {
        int readIndex = in.start;

        boolean negative = in.buffer.getByte(readIndex) == '-';

        if (negative && ++readIndex == in.end) {
          //only one single '-'
          byte[] buf = new byte[in.end - in.start];
          in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
          throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
        }

        int radix = 10;
        long max = -Long.MAX_VALUE / radix;
        long result = 0;
        int digit;

        while (readIndex < in.end) {
          digit = Character.digit(in.buffer.getByte(readIndex++),radix);
          //not valid digit.
          if (digit == -1) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
          //overflow
          if (max > result) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }

          long next = result * radix - digit;

          //overflow
          if (next > result) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
          result = next;
        }
        if (!negative) {
          result = -result;
          //overflow
          if (result < 0) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
        }

        out.isSet = 1;
        out.value = result;
      }

    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "read_int", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class ReadIntOrNull implements DrillSimpleFunc{

    @Param NullableVarCharHolder in;
    @Output NullableIntHolder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {

      if ((in.end - in.start) ==0) {
        //empty, not a valid number
        //byte[] buf = new byte[in.end - in.start];
        //in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
        out.isSet = 0;
        //throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
      }
      else {
        int readIndex = in.start;

        boolean negative = in.buffer.getByte(readIndex) == '-';

        if (negative && ++readIndex == in.end) {
          //only one single '-'
          byte[] buf = new byte[in.end - in.start];
          in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
          throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
        }

        int radix = 10;
        int max = -Integer.MAX_VALUE / radix;
        int result = 0;
        int digit;

        while (readIndex < in.end) {
          digit = Character.digit(in.buffer.getByte(readIndex++),radix);
          //not valid digit.
          if (digit == -1) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
          //overflow
          if (max > result) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }

          int next = result * radix - digit;

          //overflow
          if (next > result) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
          result = next;
        }
        if (!negative) {
          result = -result;
          //overflow
          if (result < 0) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
        }
        out.isSet = 1;
        out.value = result;
      }
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "read_int", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.INTERNAL)
  public static class ReadIntOrNullAllowIncommingNulls implements DrillSimpleFunc{

    @Param VarCharHolder in;
    @Output NullableIntHolder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {

      if ((in.end - in.start) ==0) {
        //empty, not a valid number
        //byte[] buf = new byte[in.end - in.start];
        //in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
        out.isSet = 0;
        //throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
      }
      else {
        int readIndex = in.start;

        boolean negative = in.buffer.getByte(readIndex) == '-';

        if (negative && ++readIndex == in.end) {
          //only one single '-'
          byte[] buf = new byte[in.end - in.start];
          in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
          throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
        }

        int radix = 10;
        int max = -Integer.MAX_VALUE / radix;
        int result = 0;
        int digit;

        while (readIndex < in.end) {
          digit = Character.digit(in.buffer.getByte(readIndex++),radix);
          //not valid digit.
          if (digit == -1) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
          //overflow
          if (max > result) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }

          int next = result * radix - digit;

          //overflow
          if (next > result) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
          result = next;
        }
        if (!negative) {
          result = -result;
          //overflow
          if (result < 0) {
            byte[] buf = new byte[in.end - in.start];
            in.buffer.getBytes(in.start, buf, 0, in.end - in.start);
            throw new NumberFormatException(new String(buf, com.google.common.base.Charsets.UTF_8));
          }
        }

        out.isSet = 1;
        out.value = result;
      }
    }
  }
}

 

