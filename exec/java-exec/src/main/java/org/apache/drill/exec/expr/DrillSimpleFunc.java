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

import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.record.RecordBatch;

/**
 * TODO - consider renaming to DrillUDF, as there is no corresponding complex function
 * interface, which may confuse UDF writers.
 */

/**
 * Interface for a Drill function.
 *
 * Functions included by default with Drill and UDFs are defined in the same manner, they must implement
 * this interface and (optionally? - pretty sure it is required) include an
 * {@link org.apache.drill.exec.expr.annotations.FunctionTemplate} annotation telling
 * Drill how to register the function and some of its basic properties.
 *
 * TODO - find the recent discussions about the UDF interfaces as well as Tug's blog post
 * to make sure this covers all of the current sticking points for implementing UDFs.
 *    - in all methods: setup, eval; as well as the other methods in UDAFs, reset and output fully qualified
 *      names are needed for external resources
 *    - jar must include source and class files
 *    - holders are the only things that can be used as input or output
 *
 */
public interface DrillSimpleFunc extends DrillFunc {
  /**
   * One-time function initialization steps.
   *
   * The setup method will be evaluated at least once after the function has
   * been placed in an expression tree for evaluation. In this method the {@link Param} and {@link Output}
   * members will be unavailable, although this is a constraint that cannot be enforced during UDF
   * compilation, it will fail during
   * ( TODO - registration )
   * evaluation. This method should only interact with the members
   * annotated with {@link Workspace}, as these will persist from this setup method
   * and across different evaluations of the function.
   *
   * A good example use of this method
   * would be creating an instance of a class that will be used to parse input, assuming
   * it can have it's state cleared at the top of the eval method, and that creating the object
   * is reasonably heavy-weight.
   *
   * Be aware that Drill tries to avoid allocation of objects once per
   * value in a data stream (the eval method will be called for each value it processes) to
   * avoid excessive garbage collection. Whenever possible, object allocation should be done in the {@code setup()} method, saved into
   * a member variable annotated with {@link Workspace} and re-used in the {@code eval()} method, clearing state from previous
   * evaluations if necessary.
   */
  public void setup();

  /**
   * Perform operations necessary to turn the input value into the appropriate output value.
   *
   * This function is where interaction between the members annotated with {@link Param} and
   * {@link Output} occurs. All members annotated with {@link Workspace} which should have been initialized in setup
   * will be available to aid with processing.
   *
   * Handling the basic numeric types is straight-forward, as the holders will provide you directly with java primitives you can
   * manipulate in a straightforward manner and then save back input the {@code value} field on the outgoing holder.
   *
   * Handling dates and variable length values can be a little more difficult, as Drill tries as hard as possible to avoid
   * allocating excessive objects or incurring unnecessary copies of data wherever possible. This unfortunately clashes with
   * a lot of common patterns in the java standard library and commonly used 3rd-party java libraries to use immutable objects like
   * {@link String} as inputs. When you need to work
   * with these interfaces, use the convenience methods available in {@link org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers}
   * TODO - find all of these convenience methods for decimal, time, date, timestamp, etc.
   *
   * TODO - consider allowing convenience annotations for handling strings, dates, decimals etc. without the boilerplate in every function
   * to turn a holder into data they can actually work with. Possibly would avoid headaches,
   * especially considering how unlikely many users are going to put time into optimizing these types of functions. Too
   * many interfaces in java are written against immutable objects, until we can provide alternatives we need to make working
   * with the data easy. Although we should avoid completely hiding the costs from users, as this could cause them to introduce
   * overhead where it isn't necessary, but in most cases they just need access to the data in a familiar environment.
   */
  public void eval();
}
