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

import org.apache.drill.exec.expr.holders.VarCharHolder;

/**
 * Exposes partition information for a particular storage plugin.
 *
 * For a more explanation of the current use of this interface see
 * the documentation on {@see PartitionExplorer}.
 */
public interface StoragePluginPartitionExplorer {

  /**
   * Get a list of sub-partitions under a given partition. Individual storage
   * plugins will assign specific meaning to the parameters and return
   * values. If possible, storage plugins that implement this interface
   * should return partition descriptions that are fully qualified and suitable
   * for being passed back into this interface, to view the partitions below
   * the next level of nesting.
   *
   * A return value of an empty list should be given if the partition has
   * no sub-partitions.
   *
   * Note this does cause a collision between empty partitions and leaf partitions,
   * the interface should be modified if the distinction is meaningful.
   *
   * Example: for a filesystem plugin the partition information can be simply
   * be a path from the root of the given workspace to the desired directory. The
   * return value should be defined as a list of full paths (again from the root
   * of the workspace), which can be passed by into this interface to explore
   * partitions further down. An empty list would be returned if the partition
   * provided was a file, or an empty directory.
   *
   * Note to future devs, keep this doc in sync with {@see PartitionExplorer}.
   *
   * @param workspace - name of a workspace defined under the storage plugin
   * @param partition - a partition identifier
   * @return - list of sub-partitions, will be empty if a there is not another
   *           level of sub-partitions below, i.e. hit a leaf partition
   * @returns PartitionNotFoundException when the partition does not exist in
   *          the given workspace
   */
  Iterable<String> getSubPartitions(VarCharHolder workspace, VarCharHolder partition) throws PartitionNotFoundException;
}
