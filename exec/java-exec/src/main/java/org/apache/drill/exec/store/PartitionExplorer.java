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

public interface PartitionExplorer {

  /**
   * For the storage plugin provided,
   * get a list of sub-partitions under a given partition. Individual storage
   * plugins will assign specific meaning to the parameters and return
   * values.
   *
   * A return value of an empty list should be given if the partition has
   * no sub-partitions.
   *
   * Example: for a filesystem plugin the partition information can be simply
   * a path from the root of the storage plugin in the given workspace. The
   * return value could reasonably be defined as a list of full paths, or just
   * the directory/file names defined in the given directory. An empty list
   * would be returned if the partition provided was a file.
   *
   * Note to future devs, keep this doc in sync with
   * {@see StoragePluginPartitionExplorer}.
   *
   * @param plugin - name of a storage plugin instance configuration
   * @param workspace - name of a workspace defined under the storage plugin
   * @param partition - a partition identifier
   * @return - list of sub-partitions, will be empty if a there is not another
   *           level of sub-partitions below, i.e. hit a leaf partition
   */
  String[] getSubPartitions(VarCharHolder plugin, VarCharHolder workspace, VarCharHolder partition) throws PartitionNotFoundException;
}
