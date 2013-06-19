package org.apache.drill.exec.opt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.*;
import org.apache.drill.common.logical.data.visitors.AbstractLogicalVisitor;
import org.apache.drill.common.logical.data.visitors.LogicalVisitor;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.MockScanPOP;
import org.apache.drill.exec.physical.config.MockStorePOP;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.server.DrillbitContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
        LogicalConverter converter = new LogicalConverter();
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

        @Override
        public MockScanPOP visitScan(Scan scan, Object obj) throws OptimizerException {
            List<MockScanPOP.MockScanEntry> myObjects;

            try {
                if ( scan.getStorageEngine().equals("local-logs")){
                    myObjects = scan.getSelection().getListWith(config,
                            new TypeReference<ArrayList<MockScanPOP.MockScanEntry>>() {
                    });
                }
                else{
                    myObjects = new ArrayList<>();
                    MockScanPOP.MockColumn[] cols = { new MockScanPOP.MockColumn("blah", SchemaDefProtos.MinorType.INT, SchemaDefProtos.DataMode.REQUIRED,4,4,4),
                            new MockScanPOP.MockColumn("blah_2", SchemaDefProtos.MinorType.INT, SchemaDefProtos.DataMode.REQUIRED,4,4,4)};
                    myObjects.add(new MockScanPOP.MockScanEntry(50, cols));
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new OptimizerException("Error reading selection attribute of Scan node in Logical to Physical plan conversion.");
            }

            return new MockScanPOP("http://apache.org", myObjects);
        }

        @Override
        public Screen visitStore(Store store, Object obj) throws OptimizerException {
            if ( ! store.iterator().hasNext()){
                throw new OptimizerException("Store node in logical plan does not have a child.");
            }
            return new Screen(store.iterator().next().accept(this, obj), context.getCurrentEndpoint());
        }

        @Override
        public PhysicalOperator visitProject(Project project, Object obj) throws OptimizerException {
            return project.getInput().accept(this, obj);
        }
    }
}
