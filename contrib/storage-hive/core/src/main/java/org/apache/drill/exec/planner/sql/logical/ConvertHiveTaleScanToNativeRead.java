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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import javassist.expr.FieldAccess;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
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
import org.apache.drill.exec.planner.types.DrillFixedRelDataTypeImpl;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.dfs.MagicString;
import org.apache.drill.exec.store.dfs.easy.EasyGroupScan;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveScan;
import org.apache.drill.exec.store.hive.HiveTable;
import org.apache.drill.exec.store.hive.HiveTable.HivePartition;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

import com.google.common.collect.Lists;
import org.apache.drill.exec.store.parquet.ParquetFormatConfig;
import org.apache.drill.exec.store.sys.StaticDrillTable;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class ConvertHiveTaleScanToNativeRead extends StoragePluginOptimizerRule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConvertHiveTaleScanToNativeRead.class);

  public static final StoragePluginOptimizerRule INSTANCE = new ConvertHiveTaleScanToNativeRead();
  private static final String HIVE_NULL_STR = "\\N";

  private ConvertHiveTaleScanToNativeRead() {
    super(RelOptHelper.any(DrillScanRel.class),
        "ConvertHiveTaleScanToNativeRead:Text");
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
    final HiveScan hiveScan = (HiveScan)scanRel.getGroupScan();
    final HiveReadEntry hiveReadEntry = (HiveReadEntry)scanRel.getDrillTable().getSelection();
//          final String tablePath = hiveScan.storagePlugin.getTablePathOnFs(hiveReadEntry.getTable().getTableName());
    // TODO - find an API for getting the directory where external hive tables are stored, so far I can only
    // find the show create table command but that would require me to parse it to find the path
//          final String tablePath = "/Users/jaltekruse/test_data_drill/par_hive_types";
    StorageDescriptor sd = null;
    if (hiveReadEntry.getHivePartitionWrappers().size() == 0) {
      // TODO - return valuesrel of size 0
    }
    List<String> filePaths = Lists.newArrayList();
    for (HivePartition hivePart : hiveReadEntry.getHivePartitionWrappers() ) {
      sd = hivePart.getPartition().getSd();
      filePaths.add(sd.getLocation());
    }
    final String partitionPath = sd.getLocation();
    final String tablePath = hiveReadEntry.getTable().getSd().getLocation();
    final StoragePlugin storagePlugin;
    final FormatPlugin formatPlugin;

    try {
      storagePlugin = getQueryContext().getStorage().getPlugin("dfs");
      formatPlugin = getFormatPlugin(sd, tablePath, storagePlugin);
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }
    RelDataTypeFactory typeFactory = scanRel.getCluster().getTypeFactory();
    final DrillScanRel nativeScan;

    final DynamicDrillTable dynamicDrillTable = new DynamicDrillTable(
        storagePlugin,
        "file",
        getQueryContext().getQueryUserName(),
//                new FormatSelection(formatConfig, Lists.newArrayList(tablePath)));
//                new FormatSelection(new ParquetFormatConfig(), Lists.newArrayList(tablePath)));
        new FormatSelection(formatPlugin.getConfig(), Lists.newArrayList(tablePath)));
    RelDataTypeDrillImpl anyType = new RelDataTypeDrillImpl(new RelDataTypeHolder(), typeFactory);
    final RelOptTableImpl table = RelOptTableImpl.create(
        scanRel.getTable().getRelOptSchema(),
        anyType,
        dynamicDrillTable);

//      FormatSelection formatSelection = new FormatSelection(formatPlugin.getConfig(), filePaths);
      try {
        FormatSelection formatSelection = getFormatSelection( formatPlugin, storagePlugin, filePaths, tablePath);
        GroupScan groupScan = storagePlugin.getPhysicalScan(
            getQueryContext().getQueryUserName(),
            formatPlugin,
            new JSONOptions(formatSelection),
            AbstractGroupScan.ALL_COLUMNS);
//      nativeScan = getNewScan(typeFactory, formatPlugin, storagePlugin, tablePath, scanRel);
        nativeScan = new DrillScanRel(
            scanRel.getCluster(), scanRel.getTraitSet(), table, groupScan, anyType, AbstractGroupScan.ALL_COLUMNS);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    // TODO - add casts to make the data types the same
    //      - partly done
    //      - need to change the types where needed that are stored as a different physical type
    //        like tinyint and smallint, which are both stored as int32
    // TODO - I think Drill currently does not support collation in its sort operation, but Hive
    // is exposing collation information to Calcite on columns that have it, might need to add
    // a sort as well as a project, although without collation we might not be able to reproduce the
    // same sort order
    //    - we seem to actually be adding the collation to every column when we create the type
    //      we aren't looking at the hive metadata to populate this
    final List<RexNode> rexNodes = Lists.newArrayList();
    final List<String> fieldNames = Lists.newArrayList();
    final RexBuilder rb = scanRel.getCluster().getRexBuilder();

    addColumnCasts(typeFactory, sd, fieldNames, rb, rexNodes, scanRel, nativeScan);
    addPartitionColumns(typeFactory, hiveReadEntry, fieldNames, rb, rexNodes, scanRel, nativeScan);

    RelDataType rowDataType = new DrillFixedRelDataTypeImpl(typeFactory, fieldNames);
    call.transformTo(DrillProjectRel.create(scanRel.getCluster(), scanRel.getTraitSet(), nativeScan, rexNodes,
        rowDataType));
  }

  // TODO - don't think I need this
  private FormatSelection getFormatSelection(
                                  FormatPlugin formatPlugin,
                                  StoragePlugin storagePlugin,
                                  List<String> partitionPaths,
                                  String tablePath
                                  ) throws IOException {



    DrillFileSystem fs = ImpersonationUtil.createFileSystem(getQueryContext().getQueryUserName(), ((FileSystemPlugin)storagePlugin).getFsConf());

    // TODO - review this, was previously passing the partition path instead of the table path, but this didn't set the selection root correctly
    FileSelection fileSelection = new FileSelection(partitionPaths, tablePath, true); // new FileSelection(Lists.newArrayList(tablePath), tablePath, true);
    ArrayList<BasicFormatMatcher> dirMatchers = Lists.newArrayList(
        new BasicFormatMatcher(
            formatPlugin,
            Lists.newArrayList(
                Pattern.compile(".*")),
            Lists.<MagicString>newArrayList()));
    ArrayList<BasicFormatMatcher> fileMatchers = Lists.newArrayList(
        new BasicFormatMatcher(
            formatPlugin,
            Lists.newArrayList(
                Pattern.compile(".*")),
            Lists.<MagicString>newArrayList()));

    FormatSelection formatSelection = findFiles(fileSelection, dirMatchers, fileMatchers, fs);

//    GroupScan groupScan = storagePlugin.getPhysicalScan(
//        getQueryContext().getQueryUserName(),
//        formatPlugin,
//        new JSONOptions(formatSelection),
//        AbstractGroupScan.ALL_COLUMNS);
    return formatSelection;
  }

  private void addColumnCasts(RelDataTypeFactory typeFactory,
                              StorageDescriptor sd,
                              List<String> fieldNames,
                              RexBuilder rb,
                              List<RexNode> rexNodes,
                              DrillScanRel scanRel,
                              DrillScanRel nativeScan ) {
    boolean allSelected = false;
    int columnIndex = 0;
    // TODO - This loop needs to go through the columns in the order they are stored in Hive, the column ordinals
    // are used to assign the correct names and types to data read out of text tables
//    for (RelDataTypeField field : scanRel.getRowType().getFieldList()) {
    // TODO - ensure this interface has a stable ordering for the columns that will match the order
    // they appear in raw text files
    // TODO - grab out of the table level storage descriptor rather than the partition one
    //    - what about the case of column additions, partitions may be able to have more columns than others
    //      this might cause problems as the parquet read will produce nullable ints for columns not found
    //      which may not be castable to all of the other types
    for (FieldSchema field : sd.getCols()) {
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
      fieldNames.add(field.getName());
      // This API strongly advises against hard coding false for case insensitivity, however Drill is case insensitive
      // throughout the system in everything but storage systems that require case sensitivity so we can push down
      // (like Hbase)
//      final RexNode fieldAccess = rb.makeInputRef(anyType, nativeScan.getRowType().getField(field.getName(), false, false).getIndex());
      final RelDataType repeatedVarchar = typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
      // Add a cast to the correct columns coming out of the text scan
      final RexNode fieldAccess = rb.makeCall(
          SqlStdOperatorTable.ITEM,
          rb.makeInputRef(
              repeatedVarchar,
              nativeScan.getRowType().getField("columns", false, false).getIndex()),
          rb.makeExactLiteral(new BigDecimal(columnIndex)));
      // Hive stores nulls as \N, these need to be turned into SQL NULLs
      // before casting to the appropriate type
      final RexNode removeNullsCase = rb.makeCall(
          SqlCaseOperator.INSTANCE,
          rb.makeCall(
              SqlStdOperatorTable.EQUALS,
              fieldAccess,
              rb.makeLiteral(HIVE_NULL_STR)),
          rb.makeNullLiteral(SqlTypeName.VARCHAR),
          fieldAccess);
      // TODO - make sure complex types are excluded with a useful error message elsewhere
//      PrimitiveObjectInspector.PrimitiveCategory pCat = ((PrimitiveTypeInfo)TypeInfoUtils.getTypeInfoFromTypeString(field.getType())).getPrimitiveCategory();

//      TypeInfo pType = TypeInfoUtils.getTypeInfoFromTypeString(field.getType());
      // Cast the Varchar to the appropriate type
      rexNodes.add(
          rb.makeCast(
              getDrillType(
                  scanRel.getRowType().getField(
                      field.getName(),
                      false,
                      false
                  ).getType(),
                  typeFactory),
              removeNullsCase));
      columnIndex++;
    }
  }

  private void addPartitionColumns(RelDataTypeFactory typeFactory,
                                   HiveReadEntry hiveReadEntry,
                                   List<String> fieldNames,
                                   RexBuilder rb,
                                   List<RexNode> rexNodes,
                                   DrillScanRel scanRel,
                                   DrillScanRel nativeScan ) {
    final RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    int partitionIndex = 0;
    // TODO - look at what is stored in the partition dir name for null values
    for (FieldSchema field : hiveReadEntry.getTable().getPartitionKeys()) {
      fieldNames.add(field.getName());

      // Hive stores partition values in directory names with the format field_name=value
      // TODO - strip away field names and '=' character
      // TODO - research what Hive does if the field name or value contains =
      // TODO - take a look at the stackoverflow answer that seemed to indicate you could give an
      // arbitrary path to an partition, would need to resolve this mapping
      // http://blog.zhengdong.me/2012/02/22/hive-external-table-with-partitions
      // http://stackoverflow.com/questions/15271061/is-it-possible-to-import-data-into-hive-table-without-copying-the-data/22170468#22170468

      final String dirColName = getQueryContext().getOptions().getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL).string_val + partitionIndex;
      final RexNode removeNullsCase = rb.makeCall(
          SqlStdOperatorTable.SUBSTRING,
          rb.makeCall(
              SqlStdOperatorTable.SUBSTRING,
              rb.makeInputRef(
                  varcharType,
                  nativeScan.getRowType().getField(dirColName, false, false).getIndex()),
              rb.makeLiteral("=.*$")),
          rb.makeExactLiteral(new BigDecimal(2)));
      // TODO - make sure complex types are excluded with a useful error message elsewhere
//      PrimitiveObjectInspector.PrimitiveCategory pCat = ((PrimitiveTypeInfo)TypeInfoUtils.getTypeInfoFromTypeString(field.getType())).getPrimitiveCategory();

//      TypeInfo pType = TypeInfoUtils.getTypeInfoFromTypeString(field.getType());
      // Cast the Varchar to the appropriate type
      rexNodes.add(
          rb.makeCast(
              getDrillType(
                  scanRel.getRowType().getField(
                      field.getName(),
                      false,
                      false
                  ).getType(),
                  typeFactory),
              removeNullsCase));
      partitionIndex++;
    }
  }

  // TODO - don't think I need this anymore
  private FormatSelection findFiles(FileSelection fileSelection,
                                    ArrayList<BasicFormatMatcher> dirMatchers,
                                    ArrayList<BasicFormatMatcher> fileMatchers,
                                    DrillFileSystem fs) throws IOException {
    FormatSelection formatSelection = null;
    //==========================================================================================================
    // TODO - share this code with WorkspaceSchemaFactory.create() where it was copied out of
    if (fileSelection.containsDirectories(fs)) {
      for (FormatMatcher m : dirMatchers) {
        try {
          formatSelection = m.isReadable(fs, fileSelection);
          if (formatSelection != null) {
            break;
          }
        } catch (IOException e) {
          logger.debug("File read failed.", e);
        }
      }
      fileSelection = fileSelection.minusDirectories(fs);
    }
    for (FormatMatcher m : fileMatchers) {
      formatSelection = m.isReadable(fs, fileSelection);
      if (formatSelection != null) {
        break;
      }
    }
    //==========================================================================================================
    return formatSelection;
  }

  private FormatPlugin getFormatPlugin(StorageDescriptor sd,
                                        String tablePath,
                                        StoragePlugin storagePlugin) throws ExecutionSetupException {

    final TextFormatPlugin.TextFormatConfig formatPluginConfig;
    final TextFormatPlugin formatPlugin;

    formatPluginConfig = new TextFormatPlugin.TextFormatConfig();
    // TODO - look at what is in this parameters map when reading parquet backed tables
    // TODO - make sure we only set this for text, need to ignore it for parquet
    // TODO - see if Hive allows multi-character delimiter, I do not believe so, but this does return a String, not a char
    formatPluginConfig.fieldDelimiter = sd.getSerdeInfo().getParameters().get("field.delim").charAt(0);
    formatPluginConfig.lineDelimiter = "\n";
    // This needed to be set as it need to be non-null to initialize the BasicFormatMatcher which happens down the call chain
    // from the constructor of the TextFormatPlugin below
    // The actual patterns used to find files (which currently match anything) are defined below in the FormatMatcher lists
    // TODO - probably need to update to make it ignore files starting with . and _
    formatPluginConfig.extensions = Lists.newArrayList();
//    StoragePluginConfig storagePluginConfig = getQueryContext().getStorage().getPlugin("dfs").getConfig();
    // TODO - this isn't working
    // TODO - need to make this not share format config with the rest of drill, it needs to be configured to do whatever hive is doing
    // in particular we need to not have skipHeader option set to false
    // this shouldn't be too hard, the format config is sent with the plan, there is no need to configure a fake config in the registry
//            FormatPluginConfig formatConfig = getQueryContext().getStorage().getFormatPlugin(storagePluginConfig, new TextFormatPlugin.TextFormatConfig()).getConfig();
    // TODO - this takes a long time in the debugger, might be a big part of the start up time of Drill
    formatPlugin = new TextFormatPlugin(
        "hive_native_text_scan_plugin",
        getQueryContext().getDrillbitContext(),
        new Configuration(),
        storagePlugin.getConfig(),
        formatPluginConfig);
    return formatPlugin;
  }
}
