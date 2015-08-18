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
package org.apache.drill.exec.expr;

import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.record.RecordBatch;

/**
 * Aggregate function interface.
 *
 * Intermediate state should be kept in member variables annotated with {@link Workspace}.
 *
 * TODO - figure out the best way to talk about avoiding object allocation by sharing information between
 * these docs and the ones for {@link DrillSimpleFunc} for appropriate use of the setup method.
 *
 */
public interface DrillAggFunc extends DrillFunc{
  /**
   * Initialization for the beginning of the aggregation.
   *
   */
  public void setup();

  /**
   * Process a single value of input, update intermediate aggregate state appropriately.
   */
  public void add();

  /**
   * Output the current state of the aggregation, the caller of this function has
   * determined that either the aggregation group has been completely exhausted, or would
   * like to make use of the result of the aggregation in the values so far.
   */
  public void output();

  /**
   * The aggregation is complete, clear any intermediate state to enable the next
   * call to {@link DrillAggFunc#add()} to start a fresh aggregation. A subsequent call
   * to output should get the clean aggregation state assuming no inputs so far (this could be an error or null if the
   * output is somehow undefined on no inputs), the caller of this function is assumed to have
   * called {@link DrillAggFunc#output()} and collected the result of the aggregation before calling {@code reset()}.
   * The {@link DrillAggFunc#setup()}
   * method may or many not be called after this. Drill will assume that neither cases
   * will influence the behavior of the function. Please see the {@link DrillAggFunc#setup()} for information about
   * what belongs there, and only use this method to clear state.
   *
   * TODO - Is this guarenteed to be called after setup() and before the first call to add()? I do not think
   * that UDF writers are allowed to have these functions call each other, but they might try to save duplicating code bwteen setup
   * and reset if they assume this may not be called at the beginning of evaluation.
   *
   * TODO - Should doc that none of the methods in UDFs or UDAFs can call each other, provide errors on function registration if they do
   */
  public void reset();
}
