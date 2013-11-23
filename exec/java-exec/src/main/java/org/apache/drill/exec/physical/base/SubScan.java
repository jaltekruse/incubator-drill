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

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.ReadEntry;

import java.util.List;

/**
 * A SubScan operator represents the data scanned by a particular major/minor fragment.  This is in contrast to
 * a GroupScan operator, which represents all data scanned by a physical plan.
 */
public interface SubScan extends Scan {

  /**
   * Return the list of columns that will be scanned by this <code>GroupScan</code>.
   * @return - list of columns to be scanned
   */
  public List<SchemaPath> getColumns();

  /**
   * Return the number of records to scan.
   *  Note: unlike other fields like columns and filterExpr, this will differ from the group scan
   *        to allow parallelization
   * @return - number of records to read
   */
  public long getRecordLimit();

  /**
   * Grab the filter applied to this scan operation.
   * @return - logical expression (no type information associated with columns) to be applied during read
   */
  public LogicalExpression getFilterExpr();
}
