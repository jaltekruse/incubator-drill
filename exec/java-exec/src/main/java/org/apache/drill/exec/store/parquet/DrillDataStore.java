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
package org.apache.drill.exec.store.parquet;

import java.io.IOException;

/**
 * An DrillDataStore represents a storage mechanism in memory or on disk
 * that can provide data to another source or have data written into it. As
 * Drill will be concerned with reading data in many different formats, this class
 * attempts to encapsulate all of the functionality needed for each source/sink for
 * data.
 */
public interface DrillDataStore<SUB_COMP extends DrillDataStore> {

  public abstract boolean needNewSubComponent();
  public abstract boolean getNextSubComponent() throws IOException;

  /**
   * Indicates whether this abstraction of a file contains other sub-components.
   *
   * @return - true if there are components below
   */
  public abstract boolean hasSubComponents();

  /**
   * Get the current sub-component of the data source representation.
   *
   * @return - the abstract data source below
   */
  public abstract SUB_COMP getCurrentSubComponent();

  /**
   * Indicates if data is available at this level of the file. For abstractions that are just
   * wrappers of smaller components, this will return false.
   *
   * Example, in parquet there are row groups, but the data is actually all contained in the
   * pages below the row group. The row groups is a container for a number of rows and pages
   * within rows.
   * @return
   */
  public abstract boolean dataStoredAtThisLevel();

  public void updatePositionAfterWrite(int valsWritten);

  /**
   * The length of each defined value in bytes.
   *
   * @return - if positive, the length in bytes
   * @throws - if the data in this source is not fixed length
   */
  public int getTypeLengthInBytes() throws UnsupportedOperationException;

  /**
   * The length of each defined value in bits.
   * @return
   * @throws UnsupportedOperationException
   */
  public int getTypeLengthInBits() throws UnsupportedOperationException;

}
