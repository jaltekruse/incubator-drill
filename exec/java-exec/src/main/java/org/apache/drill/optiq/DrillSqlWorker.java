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

import net.hydromatic.optiq.jdbc.ConnectionConfig;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.RuleSet;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.logical.LogicalPlan;
import org.eigenbase.rel.RelNode;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class DrillSqlWorker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlWorker.class);

  private final DrillConfig config;
  private final FunctionRegistry registry;
  private final StorageEngines engines;
  private final Planner planner;
  
  public DrillSqlWorker(DrillConfig config) throws Exception {
    this.config = config;
    this.registry = new FunctionRegistry(config);
    String enginesData = Resources.toString(Resources.getResource("storage-engines.json"), Charsets.UTF_8);
    this.engines = config.getMapper().readValue(enginesData, StorageEngines.class);
    this.planner = Frameworks.getPlanner(ConnectionConfig.Lex.MYSQL, new DrillSchemaFactory(engines, config), SqlStdOperatorTable.instance(), new RuleSet[]{DrillRuleSets.DRILL_BASIC_RULES});
    
  }

  
  public LogicalPlan getPlan(String sql) throws SqlParseException, ValidationException, RelConversionException{
    SqlNode sqlNode = planner.parse(sql);
    SqlNode validatedNode = planner.validate(sqlNode);
    RelNode relNode = planner.convert(validatedNode);
    RelNode convertedRelNode = planner.transform(0, planner.getEmptyTraitSet().plus(DrillRel.CONVENTION), relNode);
    if(convertedRelNode instanceof DrillStoreRel){
      throw new UnsupportedOperationException();
    }else{
      convertedRelNode = new DrillScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
    }
    
    DrillImplementor implementor = new DrillImplementor(new DrillParseContext(registry));
    implementor.go( (DrillRel) convertedRelNode);
    planner.close();
    planner.reset();
    return implementor.getPlan();
    
  }
  private void x() throws Exception {
    String sqlAgg = "select a, count(1) from parquet.`/Users/jnadeau/region.parquet` group by a";
    String sql = "select * from parquet.`/Users/jnadeau/region.parquet`";

    
    System.out.println(sql);
    System.out.println(getPlan(sql).toJsonString(DrillConfig.create()));
    System.out.println("///////////");
    System.out.println(sqlAgg);
    System.out.println(getPlan(sqlAgg).toJsonString(DrillConfig.create()));
  }

  public static void main(String[] args) throws Exception {
    DrillConfig config = DrillConfig.create();
    DrillSqlWorker worker = new DrillSqlWorker(config);
    worker.x();
  }

  
}
