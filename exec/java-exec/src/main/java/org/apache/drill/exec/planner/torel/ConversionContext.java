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
package org.apache.drill.exec.planner.torel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.io.Resources;

import net.hydromatic.optiq.jdbc.ConnectionConfig;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import net.hydromatic.optiq.prepare.Prepare;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;

import net.hydromatic.optiq.tools.RuleSet;
import org.apache.commons.codec.Charsets;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.*;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.exec.planner.logical.*;
import org.apache.drill.exec.planner.logical.ScanFieldDeterminer.FieldList;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptQuery;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.LogicalExpressionToRex;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;

import static net.hydromatic.optiq.jdbc.ConnectionConfig.*;
import static net.hydromatic.optiq.jdbc.ConnectionConfig.Lex.*;

// TODO - was unsure why this was implementing ToRelContext, the methods were unused so far
// and it was proving difficult to construct a RelOptCluster (trouble making a RelOptPlanner instance
// this also seemed unnecessary, as we really shouldn't need a planner with a set of rules for this
// transformation
public class ConversionContext implements ToRelContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConversionContext.class);

  private static final ConverterVisitor VISITOR = new ConverterVisitor();
  
  private final Map<Scan, FieldList> scanFieldLists;
  private final Prepare prepare;
  private final LogicalExpressionToRex logToRex;

  private final RexBuilder rexBuilder;
  private RelOptCluster cluster;
  private final StorageEngines engines;
  private final LogicalPlan plan;
  private final QueryContext queryContext;

  private final JavaTypeFactoryImpl typeFactory;

  public ConversionContext(StoragePluginRegistry.DrillSchemaFactory schemaFactory, DrillConfig config, LogicalPlan plan, QueryContext queryContext) throws ExecutionSetupException {
    super();
    scanFieldLists = ScanFieldDeterminer.getFieldLists(plan);
    this.prepare = null;
    this.queryContext = queryContext;
    this.plan = plan;
    typeFactory = new JavaTypeFactoryImpl();
    rexBuilder = new RexBuilder(typeFactory);
    DrillParseContext drillParseContext = new DrillParseContext(new FunctionRegistry(config));

    String enginesData;
    try {
      enginesData = Resources.toString(Resources.getResource("storage-engines.json"), Charsets.UTF_8);
      engines = config.getMapper().readValue(enginesData, StorageEngines.class);
      Planner planner = Frameworks.getPlanner(MYSQL, schemaFactory, SqlStdOperatorTable.instance(), new RuleSet[]{DrillRuleSets.DRILL_BASIC_RULES});
      planner.parse("select 5+5 from table_name");
      RelOptQuery query = new RelOptQuery(planner.getRelOptPlanner());
      cluster = query.createCluster(
              rexBuilder.getTypeFactory(), rexBuilder);
    } catch (IOException e) {
      throw new ExecutionSetupException(e);
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }
    this.logToRex = new LogicalExpressionToRex(rexBuilder, drillParseContext);

  }

  public JavaTypeFactoryImpl getTypeFactory() {
    return typeFactory;
  }

  public RexBuilder getRexBuilder() {
    return rexBuilder;
  }

  private FieldList getFieldList(Scan scan) {
    assert scanFieldLists.containsKey(scan);
    return scanFieldLists.get(scan);
  }
  
  public RelTraitSet getLogicalTraits(){
    RelTraitSet set = RelTraitSet.createEmpty();
    set.plus(DrillRel.CONVENTION);
    return set;
  }
  
  public RelNode toRel(LogicalOperator operator) throws InvalidRelException{
    return operator.accept(VISITOR, this);
  }
  
  public RexNode toRex(LogicalExpression e){
    try {
      return e.accept(this.logToRex, new Object());
    } catch (Exception e1) {
      // TODO - decide what exception type to throw
      throw new RuntimeException(e1);
    }
  }

  public DrillTable getTables(Store store) throws ExecutionSetupException {
    /*
    store.getTarget().getWith(queryContext.getConfig(), FormatSelection.class);
    FormatSelection formatSelection = store.getTarget().getWith(queryContext.getConfig(), FormatSelection.class);
    ArrayList<DrillTable> tables = new ArrayList(formatSelection.getAsFiles().size());
    int i = 0;
    for (String s : formatSelection.getAsFiles()){
      tables.add((DrillTable)
      i++;
    }
    */
    return (DrillTable) queryContext.getStorage().getSchemaFactory().apply(
            queryContext.getFactory().getOrphanedRootSchema()).getTable("");

    /*
    FormatSelection formatSelection = store.getTarget().getWith(queryContext.getConfig(), FormatSelection.class);
    queryContext.getStorage().getEngine().get
    StorageEngineConfig storageConfig = plan.getStorageEngineConfig(store.getStorageEngine());
    // TODO - might want to create a better implementation of drill table for both input and output
    // or find a better way of unifying the interfaces on scan and store
    String storageEngine = store.getStorageEngine();
    if (store.getStorageEngine() == null) {
      storageEngine = "mock";
    }
    return queryContext.getStorage().getEngine().
        getStorageEngine(storageConfig).getDrillTable(null, storageEngine);
        */
  }

  public List<DrillTable> getTables(Scan scan) throws ExecutionSetupException {
    FormatSelection formatSelection = scan.getSelection().getWith(queryContext.getConfig(), FormatSelection.class);
    ArrayList<DrillTable> tables = new ArrayList(formatSelection.getAsFiles().size());
    int i = 0;
    for (String s : formatSelection.getAsFiles()){
      tables.add((DrillTable)
          queryContext.getStorage().getSchemaFactory().apply(
              queryContext.getFactory().getOrphanedRootSchema()).getTable(s));
      i++;
    }
    return tables;
  }

  @Override
  public RelOptCluster getCluster() {
    return cluster;
  }

  @Override
  public RelNode expandView(RelDataType rowType, String queryString, List<String> schemaPath) {
    throw new UnsupportedOperationException();
  }

  private static class ConverterVisitor implements LogicalVisitor<RelNode, ConversionContext, InvalidRelException> {

    @Override
    public RelNode visitScan(Scan scan, ConversionContext context) throws InvalidRelException{
      try {
        return DrillScanRel.convert(scan, context);
      } catch (ExecutionSetupException e) {
        // TODO Auto-generated catch block
        throw new InvalidRelException("Problem converting logical scan to optiq rel.", e);
      }
    }

    @Override
    public RelNode visitStore(Store store, ConversionContext value) throws InvalidRelException {
      try {
        return DrillStoreRel.convert(store, value);
      } catch (ExecutionSetupException e) {
        throw new InvalidRelException("Error converting logical store to an optiq rel", e);
      }
    }

    @Override
    public RelNode visitCollapsingAggregate(CollapsingAggregate collapsingAggregate, ConversionContext value) throws InvalidRelException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RelNode visitFilter(Filter filter, ConversionContext context) throws InvalidRelException{
      return DrillFilterRel.convert(filter, context);
    }

    @Override
    public RelNode visitFlatten(Flatten flatten, ConversionContext value) throws InvalidRelException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RelNode visitProject(Project project, ConversionContext context) throws InvalidRelException{
      return DrillProjectRel.convert(project, context);
    }

    @Override
    public RelNode visitConstant(Constant constant, ConversionContext value) throws InvalidRelException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RelNode visitOrder(Order order, ConversionContext context) throws InvalidRelException{
      return DrillSortRel.convert(order, context);
    }
    
    @Override
    public RelNode visitJoin(Join join, ConversionContext context) throws InvalidRelException{
      return DrillJoinRel.convert(join, context);
    }

    @Override
    public RelNode visitLimit(Limit limit, ConversionContext context) throws InvalidRelException{
      return DrillLimitRel.convert(limit, context);
    }

    @Override
    public RelNode visitRunningAggregate(RunningAggregate runningAggregate, ConversionContext value) throws InvalidRelException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RelNode visitSegment(Segment segment, ConversionContext value) throws InvalidRelException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RelNode visitSequence(Sequence sequence, ConversionContext value) throws InvalidRelException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RelNode visitTransform(Transform transform, ConversionContext value) throws InvalidRelException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RelNode visitUnion(Union union, ConversionContext context) throws InvalidRelException{
      return DrillUnionRel.convert(union, context);
    }

    @Override
    public RelNode visitWindowFrame(WindowFrame windowFrame, ConversionContext value) throws InvalidRelException {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RelNode visitGroupingAggregate(GroupingAggregate groupBy, ConversionContext context)
        throws InvalidRelException {
      return DrillAggregateRel.convert(groupBy, context);
    }
    
  }



}
