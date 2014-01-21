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
package org.apache.drill.common.logical.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;

@JsonTypeName("option")
public class OptionSetter extends SourceOperator {

  private String name;
  private String value;
  private OptionScope scope;

  @Override
  public <T, X, E extends Throwable> T accept(LogicalVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitOptionSetter(this, value);
  }

  public static enum OptionScope {
    SESSION, GLOBAL;

    public static OptionScope resolve(String val){
      for(OptionScope os : OptionScope.values()){
        if(os.name().equalsIgnoreCase(val)) return os;
      }
      throw new ExpressionParsingException(String.format("Unable to determine option scope for value '%s'.", val));
    }
  }

  @JsonCreator
  public OptionSetter(@JsonProperty("name") String name, @JsonProperty("value") String value,
                      @JsonProperty("scope") OptionScope scope) {
    super();
    this.name = name;
    this.value = value;
    this.scope = scope;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("value")
  public String getValue() {
    return value;
  }

  @JsonProperty("scope")
  public OptionScope getScope() {
    return scope;
  }

}
