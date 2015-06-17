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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.store.dfs.FormatPlugin;

public interface StoragePlugin extends SchemaFactory {
  public boolean supportsRead();

  public boolean supportsWrite();

  public Set<StoragePluginOptimizerRule> getOptimizerRules();

  /**
   * Get the physical scan operator for the particular GroupScan (read) node.
   *
   * @param userName User whom to impersonate when when reading the contents as part of Scan.
   * @param selection The configured storage engine specific selection.
   * @return
   * @throws IOException
   */
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException;

  /**
   * Get the physical scan operator for the particular GroupScan (read) node.
   *
   * @param userName User whom to impersonate when when reading the contents as part of Scan.
   * @param selection The configured storage engine specific selection.
   * @param columns (optional) The list of column names to scan from the data source.
   * @return
   * @throws IOException
   */
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns)
      throws IOException;

  /**
   * Get the physical scan operator for the particular GroupScan (read) node and custom format config.
   *
   * This interface should not be used in most cases. The format config should come out of
   * the storage plugin registry to make sure it is up to date with whatever has been configured
   * by the user. This should only be used for cases where the system needs to override the
   * user specified configuration.
   *
   * @param userName User whom to impersonate when when reading the contents as part of Scan.
   * @param selection The configured storage engine specific selection.
   * @param selection The configured storage engine specific selection.
   * @param columns (optional) The list of column names to scan from the data source.
   * @return
   * @throws IOException
   */
  public AbstractGroupScan getPhysicalScan(String userName,
                                           FormatPlugin plugin,
                                           JSONOptions selection,
                                           List<SchemaPath> columns) throws IOException;

  public StoragePluginConfig getConfig();

}
