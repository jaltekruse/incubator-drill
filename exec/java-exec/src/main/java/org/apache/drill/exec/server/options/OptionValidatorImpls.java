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

import org.apache.commons.lang.StringUtils;
import org.apache.drill.common.exceptions.ExpressionParsingException;

public class OptionValidatorImpls {

  /**
   * Validates against a given set of enumerated values provided at construction.
   * @param <TYPE> - type of objects provided in enumerated list and for validating later.
   */
  public static class EnumeratedValueValidator<TYPE extends Comparable> extends OptionValidator<TYPE> {
    private TYPE[] validValues;

    public EnumeratedValueValidator(String optionName, DrillOptions.Category category, TYPE... validValues) {
      super(optionName, category);
      this.validValues = validValues;
    }

    @Override
    public TYPE validate(TYPE value) throws ExpressionParsingException {
      for ( TYPE val : validValues ) {
        if (val.compareTo(value) == 0){
          return value;
        }
      }
      String validVals = StringUtils.join(validValues, ',');
      throw new ExpressionParsingException("Value provided for option '" + getOptionName()
          + "' is not valid, select from [" + validVals + "]");
    }
  }

  public static class MinMaxValidator<NUM extends Comparable> extends OptionValidator<NUM>{

    boolean minSet;
    boolean maxSet;
    NUM min;
    NUM max;

    public MinMaxValidator(String name, DrillOptions.Category category, NUM min, NUM max) {
      super(name, category);
      this.min = min;
      minSet = true;
      this.max = max;
      maxSet = true;
    }

    public NUM validate(NUM value) throws ExpressionParsingException {
      if (minSet) {
        if (min.compareTo(value) > 0) {
          throw new ExpressionParsingException("Value of option '" + getOptionName() + "' must be greater than " + min);
        }
      }
      if (maxSet) {
        if (max.compareTo(value) < 0 ) {
          throw new ExpressionParsingException("Value of option '" + getOptionName() + "' must be less than " + min);
        }
      }
      return value;
    }
  }

  public static class EnumeratedStringValidator extends EnumeratedValueValidator<String> {

    public EnumeratedStringValidator(String name, DrillOptions.Category category, String... validValues) {
      super(name, category, validValues);
    }

    public String validate(String s) throws ExpressionParsingException {
      for ( String val : super.validValues ) {
        if (val.equalsIgnoreCase(s)){
          return s.toLowerCase();
        }
      }

      String validVals = StringUtils.join(super.validValues, ',');
      throw new ExpressionParsingException("Value provided for option '" + getOptionName()
          + "' is not valid, select from [" + validVals + "]");
    }
  }
}
