package org.apache.drill.optiq;

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
    this.planner = Frameworks.getPlanner(new DrillSchemaFactory(engines, config), SqlStdOperatorTable.instance(), new RuleSet[]{DrillRuleSets.DRILL_BASIC_RULES});
    
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
    String sqlAgg = "select _MAP['a'] as a, count(1) from \"parquet\".\"table1\" group by _MAP['a']";
    String sql = "select _MAP['a'] as a from \"parquet\".\"table1\"";

    
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
