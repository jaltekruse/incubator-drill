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

import java.lang.reflect.Type;
import java.util.List;

import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.prepare.Prepare.CatalogReader;
import net.hydromatic.optiq.prepare.Prepare.PreparedResultImpl;
import net.hydromatic.optiq.rules.java.EnumerableRel.Prefer;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules;
import net.hydromatic.optiq.runtime.Bindable;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableModificationRelBase.Operation;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.sql.SqlKind;

/**
 * Implementation of {@link net.hydromatic.optiq.jdbc.OptiqPrepare} for Drill.
 */
public class DrillPrepareImpl extends OptiqPrepareImpl {
  public DrillPrepareImpl() {
    super();
  }

  @Override
  protected RelOptPlanner createPlanner(Context context) {
    final RelOptPlanner planner = super.createPlanner(context);
    planner.addRule(DrillValuesRule.INSTANCE);
    planner.addRule(EnumerableDrillRule.INSTANCE);

    planner.removeRule(JavaRules.ENUMERABLE_JOIN_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_CALC_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_AGGREGATE_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_SORT_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_LIMIT_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_UNION_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_INTERSECT_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_MINUS_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_TABLE_MODIFICATION_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_VALUES_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_WINDOW_RULE);
    planner.removeRule(JavaRules.ENUMERABLE_ONE_ROW_RULE);
    return planner;
  }

  protected EnumerableRelImplementor getRelImplementor(RexBuilder rexBuilder) {
    return null;
  }

  @Override
  protected OptiqPreparingStmt getPrepare(Context context, CatalogReader catalogReader, RelDataTypeFactory typeFactory,
      Schema schema, Prefer prefer, RelOptPlanner planner, Convention resultConvention) {
    return new DrillPreparingStmt(context, catalogReader, typeFactory, schema, prefer, planner, resultConvention);
  }
  
  
  public class DrillPreparingStmt extends OptiqPreparingStmt{

    public DrillPreparingStmt(Context context, CatalogReader catalogReader, RelDataTypeFactory typeFactory,
        Schema schema, Prefer prefer, RelOptPlanner planner, Convention resultConvention) {
      super(context, catalogReader, typeFactory, schema, prefer, planner, resultConvention);
    }

    @Override
    protected PreparedResult implement(RelDataType rowType, RelNode rootRel, SqlKind sqlKind) {
      return new DrillPreparedResult(rowType, fieldOrigins, rootRel, null, false);
    }
    
  }
  
  public class DrillPreparedResult extends PreparedResultImpl implements Bindable<Object>{
    
    public DrillPreparedResult(RelDataType rowType, List<List<String>> fieldOrigins, RelNode rootRel,
        Operation tableModOp, boolean isDml) {
      super(rowType, fieldOrigins, rootRel, tableModOp, isDml);
      
    }

    @Override
    public String getCode() {
      return null;
    }

    @Override
    public Type getElementType() {
      return null;
    }

    @Override
    public Bindable<Object> getBindable() {
      return this;
    }

    @Override
    public Enumerable<Object> bind(DataContext dataContext) {
      throw new UnsupportedOperationException();
    }
  }
  
  
}
