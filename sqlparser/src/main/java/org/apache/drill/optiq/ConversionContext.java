package org.apache.drill.optiq;

import java.util.Map;

import net.hydromatic.optiq.prepare.Prepare;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.GroupingAggregate;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.Limit;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Union;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.optiq.ScanFieldDeterminer.FieldList;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;

public class ConversionContext implements ToRelContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConversionContext.class);

  private static final ConverterVisitor VISITOR = new ConverterVisitor();
  
  private final Map<Scan, FieldList> scanFieldLists;
  private final RelOptCluster cluster;
  private final Prepare prepare;

  public ConversionContext(RelOptCluster cluster, LogicalPlan plan) {
    super();
    scanFieldLists = ScanFieldDeterminer.getFieldLists(plan);
    this.cluster = cluster;
    this.prepare = null;
  }

  @Override
  public RelOptCluster getCluster() {
    return cluster;
  }

  @Override
  public Prepare getPreparingStmt() {
    return prepare;
  }

  private FieldList getFieldList(Scan scan) {
    assert scanFieldLists.containsKey(scan);
    return scanFieldLists.get(scan);
  }
  
  
  public RexBuilder getRexBuilder(){
    return cluster.getRexBuilder();
  }
  
  public RelTraitSet getLogicalTraits(){
    RelTraitSet set = RelTraitSet.createEmpty();
    set.add(DrillRel.CONVENTION);
    return set;
  }
  
  public RelNode toRel(LogicalOperator operator) throws InvalidRelException{
    return operator.accept(VISITOR, this);
  }
  
  public RexNode toRex(LogicalExpression e){
    return null;
  }
  
  public RelDataTypeFactory getTypeFactory(){
    return cluster.getTypeFactory();
  }
  
  public RelOptTable getTable(Scan scan){
    FieldList list = getFieldList(scan);
    
    return null;
  }
  
  
  private static class ConverterVisitor extends AbstractLogicalVisitor<RelNode, ConversionContext, InvalidRelException>{

    @Override
    public RelNode visitScan(Scan scan, ConversionContext context){
      return DrillScan.convert(scan, context);
    }

    @Override
    public RelNode visitFilter(Filter filter, ConversionContext context) throws InvalidRelException{
      return DrillFilterRel.convert(filter, context);
    }

    @Override
    public RelNode visitProject(Project project, ConversionContext context) throws InvalidRelException{
      return DrillProjectRel.convert(project, context);
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
    public RelNode visitUnion(Union union, ConversionContext context) throws InvalidRelException{
      return DrillUnionRel.convert(union, context);
    }

    @Override
    public RelNode visitGroupingAggregate(GroupingAggregate groupBy, ConversionContext context)
        throws InvalidRelException {
      return DrillAggregateRel.convert(groupBy, context);
    }
    
  }
}
