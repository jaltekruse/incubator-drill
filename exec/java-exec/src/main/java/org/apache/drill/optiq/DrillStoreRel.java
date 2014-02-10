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
package org.apache.drill.optiq;

import java.util.List;

import net.hydromatic.optiq.prepare.Prepare.CatalogReader;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Store;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableModificationRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

public class DrillStoreRel extends TableModificationRelBase implements DrillRel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillStoreRel.class);

  JSONOptions target;
  String storageEngineName;
  LogicalOperator input;
  private final DrillTable drillTable;

  protected DrillStoreRel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, CatalogReader catalogReader,
      RelNode child, Operation operation, List<String> updateColumnList, boolean flattened,
      JSONOptions target, String storageEngineName, LogicalOperator input) {
    super(cluster, traits, table, catalogReader, child, operation, updateColumnList, flattened);
    this.target = target;
    this.storageEngineName = storageEngineName;
    this.drillTable = table.unwrap(DrillTable.class);
    this.input = input;
  }

  public static RelNode convert(Store store, ConversionContext context) throws ExecutionSetupException, InvalidRelException {
    logger.debug("store child!!" + store.iterator().next());
    return new DrillStoreRel(context.getCluster(), null, context.getTable(store), null,
        context.toRel(store.iterator().next()), null, null, false, store.getTarget(), store.getStorageEngine(), store.getInput() );
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    Store.Builder builder = Store.builder();
    builder.storageEngine(storageEngineName);
    builder.target(target);
    builder.setInput(input);
    implementor.registerSource(new DrillTable(storageEngineName, storageEngineName,
        drillTable.getSelection(), drillTable.getStorageEngineConfig()));
    //builder.outputReference(new FieldReference("_MAP"));
    return builder.build();
  }

}
