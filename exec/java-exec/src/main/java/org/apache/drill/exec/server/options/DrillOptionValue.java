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
import org.apache.drill.exec.cache.DrillSerializable;

import java.io.*;

/**
 * Represents an available option in Drill, with a method for parsing a value from a string and a pluggable
 * nested validation object that can be added at construction time to validate options with different value
 * restrictions.
 * @param <E> - type of object for the option to store
 */
public abstract class DrillOptionValue<E> implements Cloneable, DrillSerializable {
  String optionName;
  E value;
  DrillOptions parentOptionList;
  boolean valueSet;

  public DrillOptionValue(String optionName, DrillOptions parentOptionList){
    this.optionName = optionName.toLowerCase();
    this.parentOptionList = parentOptionList;
    valueSet = false;
  }

  public DrillOptionValue(String optionName, E value, DrillOptions parentOptionList) {
    this(optionName, parentOptionList);
    this.value = value;
    this.valueSet = true;
  }

  public String getOptionName() {
    return optionName;
  }

  public E getValue() {
    return value;
  }

  public void setValue(E value) {
    this.value = value;
    this.valueSet = true;
  }

  public abstract E parse(String s) throws ExpressionParsingException;

  public String unparse(E value){
    return value.toString();
  }

  public DrillOptionValue clone(){
    DrillOptionValue o = newInstance();
    // as the validator and optionName should never be changed after creation, there
    // really isn't a need to make new copies of either in the cloned instance,
    // instead references are given to the new instance
    o.optionName = this.optionName;
    try {
    } catch (ExpressionParsingException e) {
      // should never throw an error as it is pulled from an instance of the same static class
      throw new ExpressionParsingException(e);
    }
    return o;
  }

  public abstract DrillOptionValue newInstance();

  // This method is a bit of a hack to get around handling options as POJOs in
  // the validators, but handling them as wrapped OptionValue objects in the actual value
  // storage to allow them to be plugged into the distributed cache
  public static DrillOptionValue wrapBareObject(String optionName, Object o, DrillOptions parentOptionList){
    if (o instanceof Integer) {
      return new JavaTypeOptions.IntegerOptionValue(optionName, (Integer) o, parentOptionList);
    }
    else if (o instanceof Double) {
      return new JavaTypeOptions.DoubleOptionValue(optionName, (Double) o, parentOptionList);
    }
    else if (o instanceof Float) {
      return new JavaTypeOptions.FloatOptionValue(optionName, (Float) o, parentOptionList);
    }
    else if (o instanceof Long) {
      return new JavaTypeOptions.LongOptionValue(optionName, (Long) o, parentOptionList);
    }
    else if (o instanceof String) {
      return new JavaTypeOptions.StringOptionValue(optionName, (String) o, parentOptionList);
    }
    else if (o instanceof Boolean) {
      return  new JavaTypeOptions.BooleanOptionValue(optionName, (Boolean) o, parentOptionList);
    }
    else {
      throw new ExpressionParsingException("Unsupported type stored in Drill option file for option '" + optionName + "'");
    }
  }


  /********************************************************************************
   * The methods below are used to serialize and deserialize the option values for
   * syncing them across the distributed cache.
   *
   * The cache stores a map, which implicitly stores the option names.
   *
   * The validation rules will not change while a cluster is running so they are
   * not serialized either (some might change occasionally with major version upgrades).
   *
   * This only does a serDe on the values themselves.
   ********************************************************************************/

  @Override
  public void read(DataInput input) throws IOException {
    try {
      setValue(parse(input.readUTF()));
    } catch (ExpressionParsingException e) {
      throw new IOException("Error reading value for attribute '" + optionName + "'");
    }
  }

  @Override
  public void readFromStream(InputStream input) throws IOException {
    int len = input.read();
    byte[] strBytes = new byte[len];
    input.read(strBytes);
    setValue(parse(new String(strBytes, "UTF-8")));
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(unparse(value));
  }

  @Override
  public void writeToStream(OutputStream output) throws IOException {
    String val = unparse(value);
    output.write(val.length());
    output.write(val.getBytes("UTF-8"));
  }

  /********************************************************************************
   * End of serDe methods for the distributed cache
   ********************************************************************************/
}
