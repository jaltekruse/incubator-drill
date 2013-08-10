package org.apache.drill.exec.opt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.NoArgValidator;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.SinkOperator;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.*;

import com.fasterxml.jackson.core.type.TypeReference;

public class BasicOptimizer extends Optimizer{

  private DrillConfig config;
  private QueryContext context;

  public BasicOptimizer(DrillConfig config, QueryContext context){
    this.config = config;
    this.context = context;
  }

  @Override
  public void init(DrillConfig config) {

  }

  @Override
  public PhysicalPlan optimize(OptimizationContext context, LogicalPlan plan) {
    Object obj = new Object();
    Collection<SinkOperator> roots = plan.getGraph().getRoots();
    List<PhysicalOperator> physOps = new ArrayList<PhysicalOperator>(roots.size());
    LogicalConverter converter = new LogicalConverter(plan);
    for ( SinkOperator op : roots){
      try {
        PhysicalOperator pop  = op.accept(converter, obj);
        System.out.println(pop);
        physOps.add(pop);
      } catch (OptimizerException e) {
        e.printStackTrace();
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
    }

    PlanProperties props = new PlanProperties();
    props.type = PlanProperties.PlanType.APACHE_DRILL_PHYSICAL;
    props.version = plan.getProperties().version;
    props.generator = plan.getProperties().generator;
    return new PhysicalPlan(props, physOps);
  }

  @Override
  public void close() {

  }

  public static class BasicOptimizationContext implements OptimizationContext {

    @Override
    public int getPriority() {
      return 1;
    }
  }

  private class LogicalConverter extends AbstractLogicalVisitor<PhysicalOperator, Object, OptimizerException> {

    // storing a reference to the plan for access to other elements outside of the query graph
    // such as the storage engine configs
    LogicalPlan logicalPlan;

    public LogicalConverter(LogicalPlan logicalPlan){
      this.logicalPlan = logicalPlan;
    }


    @Override
    public PhysicalOperator visitScan(Scan scan, Object obj) throws OptimizerException {
      List<MockGroupScanPOP.MockScanEntry> myObjects;

      try {
        if (scan.getStorageEngine().equals("parquet")) {
          return context.getStorageEngine(logicalPlan.getStorageEngineConfig(scan.getStorageEngine())).getPhysicalScan(scan);
        }
        if (scan.getStorageEngine().equals("local-logs")) {
          myObjects = scan.getSelection().getListWith(config,
              new TypeReference<ArrayList<MockGroupScanPOP.MockScanEntry>>() {
              });
        } else {
          myObjects = new ArrayList<>();
          MockGroupScanPOP.MockColumn[] cols = {
              new MockGroupScanPOP.MockColumn("blah", MinorType.INT, DataMode.REQUIRED, 4, 4, 4),
              new MockGroupScanPOP.MockColumn("blah_2", MinorType.INT, DataMode.REQUIRED, 4, 4, 4) };
          myObjects.add(new MockGroupScanPOP.MockScanEntry(50, cols));
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new OptimizerException(
            "Error reading selection attribute of GroupScan node in Logical to Physical plan conversion.");
      } catch (SetupException e) {
        throw new OptimizerException(
            "Storage engine not found: " + scan.getStorageEngine(), e);
      }

      return new MockGroupScanPOP("http://apache.org", myObjects);
    }

    @Override
    public Screen visitStore(Store store, Object obj) throws OptimizerException {
      if (!store.iterator().hasNext()) {
        throw new OptimizerException("Store node in logical plan does not have a child.");
      }
      return new Screen(store.iterator().next().accept(this, obj), context.getCurrentEndpoint());
    }

    @Override
    public PhysicalOperator visitProject(Project project, Object obj) throws OptimizerException {
      return project.getInput().accept(this, obj);
      // return new org.apache.drill.exec.physical.config.Project(
      // Arrays.asList(project.getSelections()), project.iterator().next().accept(this, obj));
    }

    @Override
    public PhysicalOperator visitFilter(Filter filter, Object obj) throws OptimizerException {
      TypeProtos.MajorType.Builder b = TypeProtos.MajorType.getDefaultInstance().newBuilderForType();
      b.setMode(DataMode.REQUIRED);
      b.setMinorType(MinorType.BIGINT);

      return new SelectionVectorRemover(new org.apache.drill.exec.physical.config.Filter(filter.iterator().next()
          .accept(this, obj), /* filter.getExpr() */
      new FunctionCall(FunctionDefinition.simple("alternate", new NoArgValidator(), new OutputTypeDeterminer.FixedType(
          b.build())), null, new ExpressionPosition("asdf", 1)), 1.0f));
    }

  }

}
