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

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.exec.cache.DistributedMapDeserializer;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Stores a list of all available options and their expected types/values.
 *
 * For all strings, including names and option values, case is not considered meaningful.
 */
public class DrillOptions implements Iterable<DrillOptionValue>, Cloneable, DistributedMapDeserializer<DrillOptionValue>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillOptions.class);

  public static final String  QUOTED_IDENTIFIERS = "quoted_identifiers",
                              QUERY_TIMEOUT = "query_timeout",
                              EXPLAIN_PLAN_LEVEL = "explain_plan_level",
                              EXPLAIN_PLAN_FORMAT = "explain_plan_format";

  private static CaseInsensitiveMap<OptionValidator> optionValidators;
  private CaseInsensitiveMap<DrillOptionValue> optionValues;

  @Override
  public DrillOptionValue put(String key, byte[] value) throws IOException {
    ByteArrayDataInput in = ByteStreams.newDataInput(value);
    optionValues.get(key).read(in);
    return optionValues.get(key);
  }

  @Override
  public Class getValueClass() {
    return DrillOptionValue.class;
  }

  public enum Category {
    PARSING("PARSING"),
    OPTIMIZATION("OPTIMIZATION"),
    EXECUTION("EXECUTION");

    private final String name;
    Category(String name){
      this.name = name;
    }

    public String getName(){
      return name;
    }
  }

  // Any option validators added here should accompany additions to the map population in the constructor
  // as well as the populateDefaultValues method.
  static {
    optionValidators = new CaseInsensitiveMap();
    // TODO - these are just example options that can be represented with the structures in this static class, they are not
    // implemented in the execution engine yet

    optionValidators.put(QUOTED_IDENTIFIERS, allowAllValuesValidator(QUOTED_IDENTIFIERS, Category.PARSING));
    optionValidators.put(QUERY_TIMEOUT, new OptionValidatorImpls.MinMaxValidator(QUERY_TIMEOUT, Category.EXECUTION, 10, Integer.MAX_VALUE));
    optionValidators.put(EXPLAIN_PLAN_LEVEL, new OptionValidatorImpls.EnumeratedStringValidator(EXPLAIN_PLAN_LEVEL, Category.OPTIMIZATION, "PHYSICAL", "LOGICAL"));
    optionValidators.put(EXPLAIN_PLAN_FORMAT, new OptionValidatorImpls.EnumeratedStringValidator(EXPLAIN_PLAN_FORMAT, Category.OPTIMIZATION, "TEXT", "XML", "JSON"));

    /*
    // example to show the enumerated values can be used for anything that implements compareTo including integers
    optionValidators.put("numeric_enumeration_option",
        new EnumeratedValueValidator<Integer>("numeric_enumeration_option", null, 100, 200, 300, 400, 500));
    // just and example of how a custom validator can be created using an anonymous static class
    // creates an integer option that must be greater than 0 and and a multiple of 5
    optionValidators.put("example_not_for_production", new OptionValidator<Integer>("example_not_for_production", null) {
          @Override
          public Integer validate(Integer value) throws ExpressionParsingException {
            if (value > 0 && value % 5 == 0){
              return value;
            }
            throw new ExpressionParsingException("Invalid value for option '" + getOptionName()
                + "', value must be a n integer greater than 0 and a multiple of 5");
          }
    });
    */
  }

  public DrillOptions(){
    optionValues = new CaseInsensitiveMap<>();
    // start options objects with all null values, all option validators declared in the static block above should
    // have OptionValue
    optionValues.put(QUOTED_IDENTIFIERS, new JavaTypeOptions.BooleanOptionValue(QUOTED_IDENTIFIERS, null, this));
    optionValues.put(EXPLAIN_PLAN_LEVEL, new JavaTypeOptions.StringOptionValue(EXPLAIN_PLAN_LEVEL, null, this));
    optionValues.put(EXPLAIN_PLAN_FORMAT, new JavaTypeOptions.StringOptionValue(EXPLAIN_PLAN_FORMAT, null , this));
    optionValues.put(QUERY_TIMEOUT, new JavaTypeOptions.IntegerOptionValue(QUERY_TIMEOUT, null, this));
  }

  public void populateDefaultValues(){
    optionValues.put(QUOTED_IDENTIFIERS, new JavaTypeOptions.BooleanOptionValue(QUOTED_IDENTIFIERS, true, this));
    optionValues.put(EXPLAIN_PLAN_LEVEL, new JavaTypeOptions.StringOptionValue(EXPLAIN_PLAN_LEVEL, "physical", this));
    optionValues.put(EXPLAIN_PLAN_FORMAT, new JavaTypeOptions.StringOptionValue(EXPLAIN_PLAN_FORMAT, "json", this));
    optionValues.put(QUERY_TIMEOUT, new JavaTypeOptions.IntegerOptionValue(QUERY_TIMEOUT, 1000, this));
  }

  @Override
  public Iterator<DrillOptionValue> iterator() {
    return optionValues.values().iterator();
  }

  public Iterator<String> getAvailableOptionNamesIterator(){
    return optionValidators.keySet().iterator();
  }

  public DrillOptions clone(){
    DrillOptions dOpts = new DrillOptions();
    dOpts.optionValues.clear();
    for (DrillOptionValue drillOptionValue : this.optionValues.values()){
      dOpts.optionValues.put(drillOptionValue.optionName, drillOptionValue.clone());
    }
    return dOpts;
  }

  private static class CaseInsensitiveMap<V> extends HashMap<String, V> {

    public V put(String s, V val){
      super.put(s.toLowerCase(), val);
      return val;
    }

    public V get(String s){
      return super.get(s.toLowerCase());
    }
  }

  static OptionValidator allowAllValuesValidator(String s, Category category) {
    return new OptionValidator<Object>(s, category){
      @Override public Object validate(Object value) throws ExpressionParsingException {
        return value;
      }
    };
  }

  public void setOptionWithString(String optionName, String value) throws ExpressionParsingException {
    OptionValidator validator = optionValidators.get(optionName);
    DrillOptionValue opt = optionValues.get(optionName);
    if (validator == null){
      throw new ExpressionParsingException("invalid option optionName '" + optionName + "'");
    }
    optionValues.put(optionName, DrillOptionValue.wrapBareObject(optionName, validator.validate(opt.parse(value)), this));
  }

  public DrillOptionValue getOptionValue(String name){
    return optionValues.get(name);
  }

  public Category getOptionCategory(String name) {
    return optionValidators.get(name).getCategory();
  }

  /**
   * Returns the DrillOptionValue object, which can be serialized and deserialized for use
   * in the distributed cache.
   *
   * This method should only be used for getting options that will be placed into
   * distributed cache, for usage of actual option values use the getOptionValue method.
   *
   * @param name - name of a drill option
   * @return - serDe capable representation of a DrillObject
   */
  public DrillOptionValue getSerDeOptionValue(String name){
    DrillOptionValue opt = optionValues.get(name);
    // prevents value inside of map from being tampered with accidentally by users of this method
    opt.clone();
    return opt;
  }



}