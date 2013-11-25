package org.apache.drill.optiq;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.DefaultExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.drill.exec.store.SchemaProviderRegistry;
import org.apache.drill.jdbc.StorageEngines;
import org.apache.drill.sql.client.full.FileSystemSchema;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class DrillRootSchema implements Schema{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRootSchema.class);

  private final JavaTypeFactory typeFactory;
  private final ConcurrentMap<String, Schema> schemas = Maps.newConcurrentMap();
  private final SchemaProviderRegistry registry;

  public DrillRootSchema(JavaTypeFactory typeFactory, StorageEngines engines, DrillConfig config) throws SetupException {
    super();
    this.typeFactory = typeFactory;
    this.registry = new SchemaProviderRegistry(config);
    
    for (Map.Entry<String, StorageEngineConfig> entry : engines) {
      SchemaProvider provider = registry.getSchemaProvider(entry.getValue());
      FileSystemSchema schema = new FileSystemSchema(entry.getValue(), provider, typeFactory, this, entry.getKey());
      schemas.put(entry.getKey(), schema);
    }
    logger.debug("Registered schemas for {}", schemas.keySet());
  }
  
  @Override
  public Schema getSubSchema(String name) {
    return schemas.get(name);
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  @Override
  public Schema getParentSchema() {
    return null;
  }

  @Override
  public String getName() {
    return "";
  }

  @Override
  public Collection<TableFunctionInSchema> getTableFunctions(String name) {
    return Collections.emptyList();
  }

  @Override
  public <E> Table<E> getTable(String name, Class<E> elementType) {
    return null;
  }

  @Override
  public Expression getExpression() {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryProvider getQueryProvider() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Multimap<String, TableFunctionInSchema> getTableFunctions() {
    return null;
  }

  @Override
  public Collection<String> getSubSchemaNames() {
    return schemas.keySet();
  }

  @Override
  public Map<String, TableInSchema> getTables() {
    return Collections.emptyMap();
  }
  
}
