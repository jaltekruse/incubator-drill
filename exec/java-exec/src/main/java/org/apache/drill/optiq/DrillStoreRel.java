package org.apache.drill.optiq;

import java.util.List;

import net.hydromatic.optiq.prepare.Prepare.CatalogReader;

import org.apache.drill.common.logical.data.LogicalOperator;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableModificationRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;

public class DrillStoreRel extends TableModificationRelBase implements DrillRel{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillStoreRel.class);

  protected DrillStoreRel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, CatalogReader catalogReader,
      RelNode child, Operation operation, List<String> updateColumnList, boolean flattened) {
    super(cluster, traits, table, catalogReader, child, operation, updateColumnList, flattened);
    
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    return null;
  }

}
