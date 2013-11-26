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

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;

import java.util.List;

/**
 * Builder for the scan operator
 */
public class ScanBuilder {
  private String storageEngine;
  private JSONOptions selection;
  private FieldReference outputReference;
  private List<FieldReference > columns;
  private LogicalExpression filterExpr;
  private long recordLimit;

  public ScanBuilder storageEngine(String storageEngine) {
    this.storageEngine = storageEngine;
    return this;
  }

  public ScanBuilder selection(JSONOptions selection) {
    this.selection = selection;
    return this;
  }

  public ScanBuilder outputReference(FieldReference outputReference) {
    this.outputReference = outputReference;
    return this;
  }

  public Scan build() {
    return new Scan(storageEngine, selection, outputReference, columns, filterExpr, recordLimit);
  }
}