/**
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
 */
package org.apache.drill.exec.physical.base;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * A GroupScan operator represents all data which will be scanned by a given physical
 * plan.  It is the superset of all SubScans for the plan.
 */
public interface GroupScan extends Scan, HasAffinity{

  public abstract void applyAssignments(List<DrillbitEndpoint> endpoints);

  public abstract SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException;

  @JsonIgnore
  public int getMaxParallelizationWidth();

  /**
   * Return the list of columns that will be scanned by this <code>GroupScan</code>.
   * @return - list of columns to be scanned
   */
  public List<FieldReference> getColumns();

  /**
   * Return the number of records to scan.
   * @return - number of records to read
   */
  public long getRecordLimit();

  /**
   * Grab the filter applied to this scan operation.
   * @return - logical expression (no type information associated with columns) to be applied during read
   */
  public LogicalExpression getFilterExpr();
}
