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
package org.apache.drill.common.config;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Stores a list of all available options and their expected types/values. For all strings, including names and
 * option values, case is not considered meaningful.
 */
public class DrillOptions implements Iterable<DrillOptions.DrillOption>, Cloneable{

  public static final String  QUOTED_IDENTIFIERS = "quoted_identifiers",
                              QUERY_TIMEOUT = "query_timeout",
                              EXPLAIN_PLAN_LEVEL = "explain_plan_level",
                              EXPLAIN_PLAN_FORMAT = "explain_plan_format";

  @Override
  public Iterator<DrillOption> iterator() {
    return availableOptions.values().iterator();
  }

  public DrillOptions clone(){
    DrillOptions dOpts = new DrillOptions();
    dOpts.availableOptions.clear();
    for (DrillOption drillOption : this.availableOptions.values()){
      dOpts.availableOptions.put(drillOption.optionName, drillOption.clone());
    }
    return dOpts;
  }

  public class CaseInsensitiveMap extends HashMap<String, DrillOption> {

    public DrillOption put(DrillOption val){
      super.put(val.optionName.toLowerCase(), val);
      return val;
    }

    public DrillOption get(String s){
      return super.get(s.toLowerCase());
    }
  }

  CaseInsensitiveMap availableOptions;

  public DrillOptions() {
    availableOptions = new CaseInsensitiveMap();
    // TODO - these are just example options that can be represented with the structures in this class, they are not
    // implemented in the execution engine yet

    availableOptions.put(new BooleanOption(QUOTED_IDENTIFIERS));
    availableOptions.put(new IntegerOption(QUERY_TIMEOUT,
        new MinMaxValidator(QUERY_TIMEOUT, 10, Integer.MAX_VALUE)));
    availableOptions.put(new StringEnumeratedOption(EXPLAIN_PLAN_LEVEL, "PHYSICAL", "LOGICAL"));
    availableOptions.put(new StringEnumeratedOption(EXPLAIN_PLAN_FORMAT, "TEXT", "XML", "JSON"));

    // example to show the enumerated values can be used for anything that implements compareTo including integers
    availableOptions.put(new IntegerOption("numeric_enumeration_option",
        new EnumeratedValueValidator<Integer>("numeric_enumeration_option", 100, 200, 300, 400, 500)));
    // just and example of how a custom validator can be created using an anonymous class
    // creates an integer option that must be greater than 0 and and a multiple of 5
    availableOptions.put(new IntegerOption("example_not_for_production",
        new OptionValidator<Integer>("example_not_for_production") {
      @Override
      public Integer validate(Integer value) throws Exception {
        if (value > 0 && value % 5 == 0){
          return value;
        }
        throw new Exception("Invalid value for option '" + getOptionName()
            + "', value must be a n integer greater than 0 and a multiple of 5");
      }
    }));
  }

  public void setOptionWithString(String optionName, String value) throws Exception {
    DrillOption opt = availableOptions.get(optionName);
    if (opt == null){
      throw new Exception("invalid option optionName '" + optionName + "'");
    }
    opt.setValue(opt.parse(value));
  }

  /**
   * Represents an available option in Drill, with a method for parsing a value from a string and a pluggable
   * nested validation object that can be added at construction time to validate options with different value
   * restrictions.
   * @param <E> - type of object for the option to store
   */
  public abstract class DrillOption<E> implements Cloneable, DrillSerializable{
    String optionName;
    E value;
    OptionValidator<E> validator;

    public DrillOption(String optionName, OptionValidator<E> validator) {
      this.optionName = optionName;
      this.validator = validator;
    }

    public String getOptionName() {
      return optionName;
    }

    public E validate(E value) throws Exception {
      return validator.validate(value);
    }

    public void setValue(E value) throws Exception {
      value = validate(value);
      this.value = value;
    }

    public abstract E parse(String s) throws Exception;

    public String unparse(){
      return value.toString();
    }

    public DrillOption clone(){
      DrillOption o = newInstance();
      // as the validator and optionName should never be changed after creation, there
      // really isn't a need to make new copies of either in the cloned instance,
      // instead references are given to the new instance
      o.optionName = this.optionName;
      try {
        o.setValue(this.value);
      } catch (Exception e) {
        // should never throw an error as it is pulled from an instance of the same class
        throw new RuntimeException(e);
      }
      o.validator = this.validator;
      return o;
    }

    public abstract DrillOption newInstance();

    @Override
    public void read(DataInput input) throws IOException {
      try {
        setValue(parse(input.readUTF()));
      } catch (Exception e) {
        throw new IOException("Error reading value for attribute '" + optionName + "'");
      }
    }

    @Override
    public void readFromStream(InputStream input) throws IOException {
      int len = input.read();
      byte[] strBytes = new byte[len];
      input.read(strBytes);
      try {
        setValue(parse(new String(strBytes, "UTF-8")));
      } catch (Exception e) {
        throw new IOException("Error reading value for attribute '" + optionName + "'");
      }
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeUTF(unparse());
    }

    @Override
    public void writeToStream(OutputStream output) throws IOException {
      String val = unparse();
      output.write(val.length());
      output.write(val.getBytes("UTF-8"));
    }
  }

  /**
   * Validates the values provided to Drill options.
   *
   * @param <E>
   */
  public static abstract class OptionValidator<E> {
    // Stored here as well as in the option class to allow insertion of option optionName into
    // the error messages produced by the validator
    private String optionName;

    public OptionValidator(String optionName){
      this.optionName = optionName;
    }

    /**
     * This method determines if a given value is a valid setting for an option. For options that support some
     * ambiguity in their settings, such as case-insensitivity for string options, this method returns a modified
     * version of the passed value that is considered the standard format of the option that should be used for
     * system-internal representation.
     *
     * @param value - the value to validate
     * @return - the value requested, in its standard format to be used for representing the value within Drill
     *            Example: all lower case values for strings, to avoid ambiguities in how values are stored
     *            while allowing some flexibility for users
     * @throws Exception - message to describe error with value, including range or list of expected values
     */
    public abstract E validate(E value) throws Exception;

    public String getOptionName() {
      return optionName;
    }
  }

  public static OptionValidator allowAllValuesValidator = new OptionValidator<Object>(null){
    @Override public Object validate(Object value) throws Exception {
      return value;
    }
  };

  /**
   * Validates against a given set of enumerated values provided at construction.
   * @param <TYPE> - type of objects provided in enumerated list and for validating later.
   */
  public class EnumeratedValueValidator<TYPE extends Comparable> extends OptionValidator<TYPE> {
    private TYPE[] validValues;

    public EnumeratedValueValidator(String optionName, TYPE... validValues) {
      super(optionName);
      this.validValues = validValues;
    }

    @Override
    public TYPE validate(TYPE value) throws Exception {
      for ( TYPE val : validValues ) {
        if (val.compareTo(value) == 0){
          return value;
        }
      }
      String validVals = StringUtils.join(validValues, ',');
      throw new Exception("Value provided for option '" + getOptionName()
          + "' is not valid, select from [" + validVals + "]");
    }
  }

  public class MinMaxValidator<NUM extends Comparable> extends OptionValidator<NUM>{

    boolean minSet;
    boolean maxSet;
    NUM min;
    NUM max;

    public MinMaxValidator(String name, NUM min, NUM max) {
      super(name);
      this.min = min;
      minSet = true;
      this.max = max;
      maxSet = true;
    }

    public NUM validate(NUM value) throws Exception {
      if (minSet) {
        if (min.compareTo(value) > 0) {
          throw new Exception("Value of option '" + getOptionName() + "' must be greater than " + min);
        }
      }
      if (maxSet) {
        if (max.compareTo(value) < 0 ) {
          throw new Exception("Value of option '" + getOptionName() + "' must be less than " + min);
        }
      }
      return value;
    }
  }

  public class StringOption extends DrillOption<String> {

    public StringOption(String name, OptionValidator<String> validator) {
      super(name, validator);
    }

    @Override
    public String parse(String s){
      return s;
    }

    public StringOption newInstance(){
      return new StringOption(optionName, validator);
    }
  }

  public class IntegerOption extends DrillOption<Integer> {

    public IntegerOption(String name, OptionValidator<Integer> validator) {
      super(name, validator);
    }

    @Override
    public Integer parse(String s) throws Exception {
      try {
        return Integer.parseInt(s);
      } catch (RuntimeException ex){
        throw new Exception("Error parsing integer value for attribute " + optionName);
      }
    }

    @Override
    public DrillOption newInstance() {
      return new IntegerOption(optionName, validator);
    }
  }

  public class DoubleOption extends DrillOption<Double> {

    public DoubleOption(String name, OptionValidator<Double> validator) {
      super(name, validator);
    }

    @Override
    public Double parse(String s) throws Exception {
      try {
        return Double.parseDouble(s);
      } catch (RuntimeException ex){
        throw new Exception("Error parsing double value for attribute " + optionName);
      }
    }

    @Override
    public DrillOption newInstance() {
      return new DoubleOption(optionName, validator);
    }
  }

  public class FloatOption extends DrillOption<Float> {

    public FloatOption(String name, OptionValidator<Float> validator) {
      super(name, validator);
    }

    @Override
    public Float parse(String s) throws Exception {
      try {
        return Float.parseFloat(s);
      } catch (RuntimeException ex){
        throw new Exception("Error parsing float value for attribute " + optionName);
      }
    }

    @Override
    public DrillOption newInstance() {
      return new FloatOption(optionName, validator);
    }
  }

  public class LongOption extends DrillOption<Long> {

    public LongOption(String name, OptionValidator<Long> validator) {
      super(name, validator);
    }

    @Override
    public Long parse(String s) throws Exception {
      try {
        return Long.parseLong(s);
      } catch (RuntimeException ex){
        throw new Exception("Error parsing float value for attribute " + optionName);
      }
    }

    @Override
    public DrillOption newInstance() {
      return new LongOption(optionName, validator);
    }
  }

  public class BooleanOption extends DrillOption<Boolean> {

    public BooleanOption(String optionName) {
      super(optionName, allowAllValuesValidator);
    }

    @Override
    public Boolean parse(String s) throws Exception {
      if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("on"))
        return true;
      else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("off"))
        return false;
      else
        throw new Exception("Invalid value for option '" + optionName + "' must be on or off.");
    }

    @Override
    public DrillOption newInstance() {
      return new BooleanOption(optionName);
    }
  }

  /**
   * Same idea as general EnumeratedOption, but validator for strings is case-insensitive.
   */
  public class StringEnumeratedOption extends StringOption {
    public StringEnumeratedOption(String name, String value, String... validValues) {
      super(name, new EnumeratedValueValidator<String>(name, validValues) {
        public String validate(String s) throws Exception {
          for ( String val : super.validValues ) {
            if (val.equalsIgnoreCase(s)){
              return s.toLowerCase();
            }
          }
          String validVals = StringUtils.join(super.validValues, ',');
          throw new Exception("Value provided for option '" + getOptionName()
              + "' is not valid, select from [" + validVals + "]");
        }
      });
    }
  }
}
