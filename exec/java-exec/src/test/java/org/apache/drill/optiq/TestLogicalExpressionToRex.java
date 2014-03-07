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
package org.apache.drill.optiq;

import static org.junit.Assert.assertTrue;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.eigenbase.rel.InvalidRelException;
import org.junit.Ignore;
import org.junit.Test;

public class TestLogicalExpressionToRex {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestLogicalExpressionToRex.class);

  @Test
  @Ignore
  public void testPlanConversion() throws ExecutionSetupException, InvalidRelException {

    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator())) {
      bit1.run();
      client.connect();
      DrillbitContext context = bit1.getContext();
      LogicalPlan logicalPlan = context.getPlanReader().readLogicalPlan(FileUtils.getResourceAsString("/parquet_nullable.json"));
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