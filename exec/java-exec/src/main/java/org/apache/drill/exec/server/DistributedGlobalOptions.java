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

public class DistributedGlobalOptions {

  // This object is used both to validate option values and store session local options in memory.
  // It is reused here to prevent over-complicating the design to make the validation code accessible
  // to both distributed global options and session local options.
  // This also prevents the need for serializing the validation rules, as they will not change as a
  // Drill cluster is running.
  DrillOptions optionValidator;
  // actual distributed cache storage for option values
  DistributedMap<DrillOptions.DrillOptionValue> distributedCacheOptions;

  DistributedGlobalOptions(DistributedCache cache){
    distributedCacheOptions = cache.getMap(DrillOptions.DrillOptionValue.class);
    optionValidator = new DrillOptions();
    // TODO - read this from a config file
    optionValidator.setDefaults();
    // fill in setting values that are not already present in the distributed cache
    for (DrillOptions.DrillOptionValue opt : optionValidator){
      distributedCacheOptions.putIfAbsent(opt.getOptionName(), opt);
    }
  }

  public void setOption(String name, String value) throws ExpressionParsingException {
    // throws and error if provided value is not valid for setting
    optionValidator.setOptionWithString(name, value);
    // some options allow for ambiguity for user input, such as allow casing insensitive string
    // literals, to represent options consistently within the system the above method will store
    // the provided value in the standard representation, grab it below for insertion
    DrillOptions.DrillOptionValue opt = optionValidator.getSerDeOptionValue(name);
    distributedCacheOptions.put(name, opt);
  }

  /**
   * Returns the object value of the requested option.
   *
   * @param name - name of a drill option
   * @return - the value of the option
   */
  public Object getOption(String name){
    return distributedCacheOptions.get(name).getValue();
  }

}
