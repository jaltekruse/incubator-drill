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
package org.apache.drill.exec.planner.sql.logical;

import java.util.LinkedList;
import java.util.List;

import javassist.expr.FieldAccess;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DirPathBuilder;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.planner.logical.PartitionPruningUtil;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.sql.HivePartitionDescriptor;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveScan;
import org.apache.drill.exec.store.hive.HiveTable;
import org.apache.drill.exec.store.hive.HiveTable.HivePartition;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

import com.google.common.collect.Lists;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.apache.drill.exec.store.sys.StaticDrillTable;

public abstract class ConvertHiveTaleScanToNativeRead extends StoragePluginOptimizerRule {

  private ConvertHiveTaleScanToNativeRead(
      RelOptRuleOperand operand,
      String id ) {
    super(operand, id);
  }

  RelDataType getDrillType(RelDataType type, RelDataTypeFactory factory) {
    switch (type.getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
        return factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.INTEGER), true);
      default:
        return type;
    }
  }

//  public static final StoragePluginOptimizerRule HIVE_FILTER_ON_SCAN =
  public static StoragePluginOptimizerRule instance() {
      return new ConvertHiveTaleScanToNativeRead(
          RelOptHelper.any(DrillScanRel.class),
          "ConvertHiveTaleScanToNativeRead:Text") {

        @Override
        public boolean matches(RelOptRuleCall call) {
          // TODO - add restriction to check the backing file type for text or parquet
          final DrillScanRel scan = (DrillScanRel) call.rel(0);
          GroupScan groupScan = scan.getGroupScan();
          return groupScan instanceof HiveScan;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
          final DrillScanRel scanRel = (DrillScanRel) call.rel(0);
          // TODO - think about how to set storage plugin, might need to just have a system/session option
          // to configure it in the case of multiple fs plugins
          // would be nice to look through the storage plugin registry to avoid the need to set it
          // in the configuration separately if there is only one
//          StaticDrillTable staticDrillTable = new StaticDrillTable("hive", "dfs", );
          DynamicDrillTable dynamicDrillTable = null;
          final HiveScan hiveScan = (HiveScan)scanRel.getGroupScan();
          final HiveReadEntry hiveReadEntry = (HiveReadEntry)scanRel.getDrillTable().getSelection();
//          final String tablePath = hiveScan.storagePlugin.getTablePathOnFs(hiveReadEntry.getTable().getTableName());
          // TODO - find an API for getting the directory where external hive tables are stored, so far I can only
          // find the show create table command but that would require me to parse it to find the path
          final String tablePath = "/Users/jaltekruse/test_data_drill/par_hive_types";
          try {
            dynamicDrillTable = new DynamicDrillTable(
                getQueryContext().getStorage().getPlugin("dfs"),
                "file",
                getQueryContext().getQueryUserName(),
                new FormatSelection(new ParquetFormatConfig(), Lists.newArrayList(tablePath)));
          } catch (ExecutionSetupException e) {
            throw new RuntimeException(e);
          }
          RelDataTypeFactory typeFactory = scanRel.getCluster().getTypeFactory();
         RelDataTypeDrillImpl anyType = new RelDataTypeDrillImpl(new RelDataTypeHolder(), typeFactory);
          // TODO - fill in DataType for columns array in text scan
          final RelOptTableImpl table = RelOptTableImpl.create(
              scanRel.getTable().getRelOptSchema(),
              anyType,
              dynamicDrillTable);
          final DrillScanRel nativeScan = new DrillScanRel(scanRel.getCluster(), scanRel.getTraitSet(), table);
          // TODO - add casts to make the data types the same
          //      - partly done
          //      - need to change the types where needed that are stored as a different physical type
          //        like tinyint and smallint, which are both stored as int32
          // TODO - I think Drill currently does not support collation in its sort operation, but Hive
          // is exposing collation information to Calcite on columns that have it, might need to add
          // a sort as well as a project, although without collation we might not be able to reproduce the
          // same sort order
          final List<RexNode> rexNodes = Lists.newArrayList();
          final RexBuilder rb = scanRel.getCluster().getRexBuilder();
          boolean allSelected = false;
          for (RelDataTypeField field : scanRel.getRowType().getFieldList()) {
            boolean selected = false;
            for (SchemaPath sp : scanRel.getColumns()) {
              if (sp.toExpr().equals(GroupScan.ALL_COLUMNS.get(0).toExpr())) {
                allSelected = true;
                break;
              } else if (sp.getRootSegment().equals(field.getName())) {
                selected = true;
              }
            }
            if (! (selected || allSelected)) {
              continue;
            }
            // This API strongly advises against hard coding false for case insensitivity, however Drill is case insensitive
            // throughout the system in everything but storage systems that require case sensitivity so we can push down
            // (like Hbase)
            final RexNode fieldAccess = rb.makeInputRef(anyType, nativeScan.getRowType().getField(field.getName(), false, false).getIndex());
            rexNodes.add(rb.makeCast(getDrillType(field.getType(), typeFactory), fieldAccess));
          }
          call.transformTo(DrillProjectRel.create(scanRel.getCluster(), scanRel.getTraitSet(), nativeScan, rexNodes,
              nativeScan .getRowType()));
        }
      };
  }



}
