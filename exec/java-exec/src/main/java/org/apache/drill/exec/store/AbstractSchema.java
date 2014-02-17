package org.apache.drill.exec.store;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import net.hydromatic.linq4j.expressions.DefaultExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.TableFunction;

import org.apache.drill.exec.planner.logical.DrillTable;

public abstract class AbstractSchema implements Schema{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractSchema.class);

  private final SchemaHolder parentSchema;

  protected final String name;
  private static final Expression EXPRESSION = new DefaultExpression(Object.class);

  public AbstractSchema(SchemaHolder parentSchema, String name) {
    super();
    this.parentSchema = parentSchema;
    this.name = name;
  }

  
  @Override
  public SchemaPlus getParentSchema() {
    return parentSchema.getSchema();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Collection<TableFunction> getTableFunctions(String name) {
    return Collections.emptyList();
  }

  @Override
  public Set<String> getTableFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Collections.emptySet();
  }

  @Override
  public Expression getExpression() {
    return EXPRESSION;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public abstract DrillTable getTable(String name);

  @Override
  public Set<String> getTableNames() {
    return Collections.emptySet();
  }
  
  
  
}
