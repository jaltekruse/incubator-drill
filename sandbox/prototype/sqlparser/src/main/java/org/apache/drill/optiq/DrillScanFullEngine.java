package org.apache.drill.optiq;

import org.apache.drill.jdbc.DrillTableFullEngine;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Scan of a Drill table.
 */
public class DrillScanFullEngine extends TableAccessRelBase implements DrillFullEngineRel {
  private final DrillTableFullEngine drillTable;

  /**
   * Creates a DrillScan.
   */
  public DrillScanFullEngine(RelOptCluster cluster,
                             RelTraitSet traits,
                             RelOptTable table) {
    super(cluster, traits, table);
    assert getConvention() == CONVENTION;
    this.drillTable = table.unwrap(DrillTableFullEngine.class);
    assert drillTable != null;
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    DrillFullEngineOptiq.registerStandardPlannerRules(planner);
  }

  public void implement(DrillFullEngineImplementor implementor) {
    final ObjectNode node = implementor.mapper.createObjectNode();
    node.put("op", "scan");
    node.put("memo", "initial_scan");
    node.put("ref", "_MAP"); // output is a record with a single field, '_MAP'
    node.put("storageengine", drillTable.getStorageEngineName());
    node.put("selection", implementor.mapper.convertValue(drillTable.selection, JsonNode.class));
    implementor.add(node);
  }
}

// End DrillScan.java
