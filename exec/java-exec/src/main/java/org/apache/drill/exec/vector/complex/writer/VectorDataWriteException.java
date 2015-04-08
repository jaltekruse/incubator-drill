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
package org.apache.drill.exec.vector.complex.writer;

import org.apache.drill.common.types.TypeProtos;

/**
 * Exception used by the {@link FieldWriter} interface in the case
 * of an exception during population of a
 * {@link org.apache.drill.exec.vector.ValueVector}.
 *
 * The fields defined are useful for consumers of the {@link FieldWriter}
 * and related interfaces to provide the most useful feedback for the
 * type of error that occurred.
 *
 * Examples of errors possible include changes in type, changes in
 * record structure or even API misuse. Any API misuse should be
 * caught in debugging a new consumer of the {@link FieldWriter},
 * but this same interface is shared between low level errors and
 * errors expected to be propagated back to users in some form.
 * This is primarily due to the overlap in information needed in both
 * cases, with information about record structure being useful both
 * to developers as well as users trying to debug poorly structured
 * files or data fields.
 *
 * Errors may be recoverable, in operations that allow for discarding
 * a small number of erroneous records. The information given back up
 * to the provider of the incorrect data may allow for recommendations
 * of configuration or data restructuring as a workaround.
 *
 * One example is the JSON reader, which by default reads data in
 * the standard types String, Numeric, boolean, Object and List.
 * Changes in type are not currently supported in Drill, but to
 * allow more data to be processed, an option to read all scalar types
 * as varchar is provided. Some type changes however cannot be supported
 * even with this option, namely a change in structure from a scalar
 * to a complex type like map or array, or similarly between different
 * complex types such as a change from list to a map or nested
 * list/map to a differently structured combination of nested lists
 * and maps.
 *
 */
public class VectorDataWriteException extends Exception {

  TypeProtos.MajorType previousType;
  TypeProtos.MajorType newType;

  // TODO - decide if I should include this
  // For complex types a description of the nested structure
  // should be provided here for debugging
//  String structureDescription;
  public VectorDataWriteException(String message) {
    super(message);
  }

  public VectorDataWriteException(String message, TypeProtos.MajorType previousType, TypeProtos.MajorType newType) {
    super(message);
    this.previousType = previousType;
    this.newType = newType;
  }
}
