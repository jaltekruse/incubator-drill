package org.apache.drill.exec.opt;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.junit.Test;

public class BasicOptimizerTest {

    @Test
         public void parseSimplePlan() throws Exception{
    DrillConfig c = DrillConfig.create();
    LogicalPlan plan = LogicalPlan.parse(c, FileUtils.getResourceAsString("/scan_screen_logical.json"));
    System.out.println(plan.unparse(c));
    //System.out.println( new BasicOptimizer(DrillConfig.create()).convert(plan).unparse(c.getMapper().writer()));
  }

  @Test
  public void parseParquetPlan() throws Exception{
    DrillConfig c = DrillConfig.create();
    LogicalPlan plan = LogicalPlan.parse(c, FileUtils.getResourceAsString("/parquet_scan_screen.json"));
    System.out.println(plan.unparse(c));
    //System.out.println( new BasicOptimizer(DrillConfig.create()).convert(plan).unparse(c.getMapper().writer()));
  }
}
