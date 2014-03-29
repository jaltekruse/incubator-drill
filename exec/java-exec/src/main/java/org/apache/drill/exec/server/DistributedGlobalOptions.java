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
package org.apache.drill.exec.server;

import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.server.options.DrillOptionValue;
import org.apache.drill.exec.server.options.DrillOptions;

import java.util.Iterator;

// TODO - this currently is not working, as the implementation of the distributed cache currently
// relies on consistent serialization and deserialization methods for all values in the map.
// As we are storing the various options in their native types we need to hook together the
// parsing methods for the various types with the distributed cache.
public class DistributedGlobalOptions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DistributedGlobalOptions.class);

  // This object is used both to validate option values and store session local options in memory.
  // It is reused here to prevent over-complicating the design to make the validation code accessible
  // to both distributed global options and session local options.
  // This also prevents the need for serializing the validation rules, as they will not change as a
  // Drill cluster is running.
  DrillOptions optionValidator;
  // actual distributed cache storage for option values
  DistributedMap<DrillOptionValue> distributedCacheOptions;

  DistributedGlobalOptions(DistributedCache cache){
    optionValidator = new DrillOptions();
    distributedCacheOptions = cache.getMap(DrillOptionValue.class, optionValidator);
    optionValidator.populateDefaultValues();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    for (DrillOptionValue opt : optionValidator){
      distributedCacheOptions.putIfAbsent(opt.getOptionName(), opt);
    }
  }

  public void setOption(String name, String value) throws ExpressionParsingException {
    // throws and error if provided value is not valid for setting
    optionValidator.setOptionWithString(name, value);
    // some options allow for ambiguity for user input, such as allow casing insensitive string
    // literals, to represent options consistently within the system the above method will store
    // the provided value in the standard representation, grab it below for insertion
    DrillOptionValue opt = optionValidator.getSerDeOptionValue(name);
    // pulling the name out of the opt object centralized the case sensitivity policy
    distributedCacheOptions.put(opt.getOptionName(), opt);
  }

  /**
   * Returns the object value of the requested option.
   *
   * @param name - name of a drill option
   * @return - the value of the option
   */
  public Object getOption(String name){
    DrillOptionValue opt = getOptionValue(name);
    // pulling the name out of the opt object centralized the case sensitivity policy
    return distributedCacheOptions.get(opt.getOptionName()).getValue();
  }

  private DrillOptionValue getOptionValue(String name) {
    DrillOptionValue opt = optionValidator.getOptionValue(name);
    if (opt == null) {
      throw new RuntimeException("No option available with the name '" + name + "'");
    }
    return opt;
  }

  public String getOptionStringVal(String name) {
    // result is ignored, but this checks to make sure that the option name is valid
    // before grabbing it out of the distributed map
    getOptionValue(name);
    DrillOptionValue opt = distributedCacheOptions.get(name);
    return opt.unparse(opt.getValue());
  }

  public DrillOptions.Category getOptionCategory(String name){
    return optionValidator.getOptionCategory(name);
  }

  public Iterator<String> getAvailableOptionNamesIterator(){
    return optionValidator.getAvailableOptionNamesIterator();
  }

}
