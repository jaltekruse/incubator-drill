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
package org.apache.drill.exec.server.options;

import org.apache.drill.common.exceptions.ExpressionParsingException;

public class JavaTypeOptions {

  public static class StringOptionValue extends DrillOptionValue<String> {

    public StringOptionValue(String name, String value, DrillOptions parentOptionList) {
      super(name, value, parentOptionList);
    }

    public StringOptionValue(String name, DrillOptions parentOptionList){
      super(name, parentOptionList);
    }

    @Override
    public String parse(String s){
      return s;
    }

    public StringOptionValue newInstance(){
      return new StringOptionValue(optionName, value, parentOptionList);
    }
  }

  public static class IntegerOptionValue extends DrillOptionValue<Integer> {

    public IntegerOptionValue(String name, Integer value, DrillOptions parentOptionList) {
      super(name, value, parentOptionList);
    }

    public IntegerOptionValue(String name, DrillOptions parentOptionList){
      super(name, parentOptionList);
    }

    @Override
    public Integer parse(String s) throws ExpressionParsingException {
      try {
        return Integer.parseInt(s);
      } catch (ExpressionParsingException ex){
        throw new ExpressionParsingException("Error parsing integer value for attribute " + optionName);
      }
    }

    @Override
    public DrillOptionValue newInstance() {
      return new IntegerOptionValue(optionName, value, parentOptionList);
    }
  }

  public static class DoubleOptionValue extends DrillOptionValue<Double> {

    public DoubleOptionValue(String name, Double value, DrillOptions parentOptionList) {
      super(name, value, parentOptionList);
    }

    public DoubleOptionValue(String name, DrillOptions parentOptionList){
      super(name, parentOptionList);
    }

    @Override
    public Double parse(String s) throws ExpressionParsingException {
      try {
        return Double.parseDouble(s);
      } catch (ExpressionParsingException ex){
        throw new ExpressionParsingException("Error parsing double value for attribute " + optionName);
      }
    }

    @Override
    public DrillOptionValue newInstance() {
      return new DoubleOptionValue(optionName, value, parentOptionList);
    }
  }

  public static class FloatOptionValue extends DrillOptionValue<Float> {

    public FloatOptionValue(String name, Float value, DrillOptions parentOptionList) {
      super(name, value, parentOptionList);
    }

    public FloatOptionValue(String name, DrillOptions parentOptionList){
      super(name, parentOptionList);
    }

    @Override
    public Float parse(String s) throws ExpressionParsingException {
      try {
        return Float.parseFloat(s);
      } catch (ExpressionParsingException ex){
        throw new ExpressionParsingException("Error parsing float value for attribute " + optionName);
      }
    }

    @Override
    public DrillOptionValue newInstance() {
      return new FloatOptionValue(optionName, value, parentOptionList);
    }
  }

  public static class LongOptionValue extends DrillOptionValue<Long> {

    public LongOptionValue(String name, Long value, DrillOptions parentOptionList) {
      super(name, value, parentOptionList);
    }

    public LongOptionValue(String name, DrillOptions parentOptionList){
      super(name, parentOptionList);
    }

    @Override
    public Long parse(String s) throws ExpressionParsingException {
      try {
        return Long.parseLong(s);
      } catch (ExpressionParsingException ex){
        throw new ExpressionParsingException("Error parsing float value for attribute '" + optionName + "'");
      }
    }

    @Override
    public DrillOptionValue newInstance() {
      return new LongOptionValue(optionName, value, parentOptionList);
    }
  }

  public static class BooleanOptionValue extends DrillOptionValue<Boolean> {

    public BooleanOptionValue(String optionName, Boolean value, DrillOptions parentOptionList) {
      super(optionName, value, parentOptionList);
    }

    public BooleanOptionValue(String name, DrillOptions parentOptionList){
      super(name, parentOptionList);
    }

    @Override
    public Boolean parse(String s) throws ExpressionParsingException {
      if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("on"))
        return true;
      else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("off"))
        return false;
      else
        throw new ExpressionParsingException("Invalid value for option '" + optionName + "' must be on or off.");
    }

    @Override
    public DrillOptionValue newInstance() {
      return new BooleanOptionValue(optionName, value, parentOptionList);
    }
  }
}
