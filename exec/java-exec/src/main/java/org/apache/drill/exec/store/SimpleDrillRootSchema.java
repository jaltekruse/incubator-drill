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
package org.apache.drill.exec.store;

import com.beust.jcommander.internal.Lists;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.jdbc.SimpleOptiqSchema;

import java.util.ArrayList;
import java.util.List;

public class SimpleDrillRootSchema extends AbstractSchema{

  public SimpleDrillRootSchema(List<String> parentSchemaPath, String name) {
    super(parentSchemaPath, name);
  }

  public static AbstractSchema createRootSchema(boolean addMetadataSchema) {
    SimpleDrillRootSchema rootSchema =
        new SimpleDrillRootSchema(new ArrayList<String>(), "");
    // TODO - In the calcite root schema creation, this gets wrapped in the
    // SchemaPlus interface
    return rootSchema;
  }

  @Override
  public String getTypeName() {
    return null;
  }

  @Override
  public AbstractSchema getSubSchema(String name) {
    return super.getSubSchema(name);
  }
}
