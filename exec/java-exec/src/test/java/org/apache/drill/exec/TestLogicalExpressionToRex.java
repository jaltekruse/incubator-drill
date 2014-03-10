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
package org.apache.drill.exec;

import static org.junit.Assert.assertTrue;

import mockit.Expectations;
import mockit.Injectable;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.logical.*;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.LogicalExpressionToRex;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class TestLogicalExpressionToRex {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestLogicalExpressionToRex.class);

  @Test
  public void testExprConversion(@Injectable final DrillProjectRel relNode) throws IOException {
    new Expectations() {
      {
      }
    };

    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    DrillConfig config = DrillConfig.create();
    DrillParseContext drillParseContext = new DrillParseContext(new FunctionRegistry(config));
    LogicalExpressionToRex logToRex = new LogicalExpressionToRex(rexBuilder, drillParseContext);
    DrillOptiq dOptiq = new DrillOptiq();


    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator())) {
      bit1.run();
      client.connect();
      DrillbitContext context = bit1.getContext();
      //logger.debug("original plan {}", logicalPlan.unparse(context.getConfig()));
      QueryContext qContext = new QueryContext(QueryId.getDefaultInstance(), context);
      StoragePluginRegistry reg = new StoragePluginRegistry(bit1.getContext());

      LogicalPlan logicalPlan = context.getPlanReader().readLogicalPlan(FileUtils.getResourceAsString("/optiq/logical_expr_to_rex.json"));
      logger.debug("logical plan {}", logicalPlan.unparse(config));
      for (LogicalOperator lop : logicalPlan.getSortedOperators()){
        if (lop instanceof Project){
          Project proj = (Project) lop;
          for (NamedExpression ex : proj.getSelections()){
            RexNode rexNode = ex.getExpr().accept(logToRex, new Object());
            // TODO - cannot pass null below, not sure if I can mock this well
            LogicalExpression logExConverted = DrillOptiq.toDrill(drillParseContext, relNode,rexNode);

          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }


  }

  @Test
  @Ignore
  public void testPlanConversion() throws ExecutionSetupException, InvalidRelException {

    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();
    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator())) {
      bit1.run();
      client.connect();
      DrillbitContext context = bit1.getContext();
      LogicalPlan logicalPlan = context.getPlanReader().readLogicalPlan(FileUtils.getResourceAsString("/optiq/logical_expr_to_rex.json"));
      //logger.debug("original plan {}", logicalPlan.unparse(context.getConfig()));
      QueryContext qContext = new QueryContext(QueryId.getDefaultInstance(), context);
      StoragePluginRegistry reg = new StoragePluginRegistry(bit1.getContext());
      ConversionContext conversionContext = new ConversionContext(reg.getSchemaFactory(), context.getConfig(), logicalPlan, qContext);
      DrillImplementor drillImplementor = new DrillImplementor(new DrillParseContext(new FunctionRegistry(context.getConfig())), PlanProperties.Generator.ResultMode.LOGICAL);
      DrillRel relNode = (DrillRel)conversionContext.toRel(logicalPlan.getGraph().getRoots().iterator().next());
      drillImplementor.go(relNode);
      LogicalPlan convertedPlan = drillImplementor.getPlan();
      assertTrue("Converted plan does not match original", convertedPlan.equals(logicalPlan));
      logger.debug("optiq converted logical {}", convertedPlan.unparse(context.getConfig()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}