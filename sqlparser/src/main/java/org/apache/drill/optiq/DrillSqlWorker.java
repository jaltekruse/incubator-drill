package org.apache.drill.optiq;

import java.util.Collections;
import java.util.List;

import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.jdbc.OptiqPrepare.PrepareResult;
import net.hydromatic.optiq.jdbc.OptiqPrepare.SparkHandler;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.jdbc.StorageEngines;
import org.apache.drill.optiq.DrillPrepareImpl.DrillPreparedResult;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptQuery;
import org.eigenbase.rex.RexBuilder;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class DrillSqlWorker {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlWorker.class);

  private final RelOptPlanner planner;
  private final DrillPrepareImpl prepare;
  private final JavaTypeFactory typeFactory;
  private final RexBuilder rexBuilder;
  private final DrillRootSchema rootSchema;
  private final OptiqPrepare.Context ctxt;
  private final DrillConfig config;
  private final FunctionRegistry registry;

  public DrillSqlWorker(DrillConfig config) throws Exception {
    this.config = config;
    this.registry = new FunctionRegistry(config);
    this.prepare = new DrillPrepareImpl();
    this.ctxt = new Ctxt();
    this.planner = prepare.createPlanner(ctxt);
    this.typeFactory = new JavaTypeFactoryImpl();
    this.rexBuilder = new RexBuilder(typeFactory);
    String enginesData = Resources.toString(Resources.getResource("storage-engines.json"), Charsets.UTF_8);
    StorageEngines engines = config.getMapper().readValue(enginesData, StorageEngines.class);
    this.rootSchema = new DrillRootSchema(typeFactory, engines, config);

  }

  private DrillRel convert(LogicalPlan plan) {
    RelOptQuery query = new RelOptQuery(planner);
    RelOptCluster cluster = query.createCluster(typeFactory, rexBuilder);
    return null;
  }

  private void x() throws Exception {
    String sql = "select _MAP['a'] as a, count(1) from \"parquet\".\"table1\" group by _MAP['a']";
    PrepareResult<?> result = prepare.prepareSql(new Ctxt(), sql, null, Object[].class, 100);
    
    DrillPreparedResult pResult = (DrillPreparedResult) result.getBindable();
    DrillRel rootRel = (DrillRel) pResult.getRootRel();

    DrillImplementor implementor = new DrillImplementor(new DrillParseContext(registry));
    implementor.go(rootRel);
    LogicalPlan plan = implementor.getPlan();
    System.out.println(plan.toJsonString(DrillConfig.create()));
  }

  public static void main(String[] args) throws Exception {
    DrillConfig config = DrillConfig.create();
    DrillSqlWorker worker = new DrillSqlWorker(config);
    worker.x();
  }

  private class Ctxt implements OptiqPrepare.Context {

    public Ctxt() {
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
      return typeFactory;
    }

    @Override
    public Schema getRootSchema() {
      return rootSchema;
    }

    @Override
    public List<String> getDefaultSchemaPath() {
      return Collections.singletonList("classpath");
    }

    @Override
    public SparkHandler spark() {
      return OptiqPrepare.Dummy.getSparkHandler();
    }

    @Override
    public DataContext createDataContext() {
      return new DrillDataContext();
    }

    @Override
    public net.hydromatic.optiq.jdbc.ConnectionConfig config() {
      return new CCfg();
    }

  }

  private class DrillDataContext implements DataContext {

    @Override
    public Schema getRootSchema() {
      return rootSchema;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
      return typeFactory;
    }

    @Override
    public Object get(String name) {
      return null;
    }

  }

  private class CCfg implements net.hydromatic.optiq.jdbc.ConnectionConfig {

    @Override
    public boolean autoTemp() {
      return false;
    }

    @Override
    public boolean materializationsEnabled() {
      return false;
    }

    @Override
    public String model() {
      return null;
    }

    @Override
    public String schema() {
      return "default";
    }

    @Override
    public boolean spark() {
      return false;
    }

  }
}
