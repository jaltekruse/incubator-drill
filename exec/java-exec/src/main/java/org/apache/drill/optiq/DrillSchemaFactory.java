package org.apache.drill.optiq;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.drill.exec.store.SchemaProviderRegistry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

class DrillSchemaFactory  implements Function1<SchemaPlus, Schema>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSchemaFactory.class);

  private final SchemaProviderRegistry registry;
  private final Map<String, StorageEngineEntry> preEntries = Maps.newHashMap();
  
  public DrillSchemaFactory(StorageEngines engines, DrillConfig config) throws SetupException {
    super();
    this.registry = new SchemaProviderRegistry(config);
    
    for (Map.Entry<String, StorageEngineConfig> entry : engines) {
      SchemaProvider provider = registry.getSchemaProvider(entry.getValue());
      preEntries.put(entry.getKey(), new StorageEngineEntry(entry.getValue(), provider));
    }
    
  }

  public Schema apply(SchemaPlus root) {
    List<String> schemaNames = Lists.newArrayList();
    Schema defaultSchema = null;
    for(Entry<String, StorageEngineEntry> e : preEntries.entrySet()){
      FileSystemSchema schema = new FileSystemSchema(e.getValue().getConfig(), e.getValue().getProvider(), root, e.getKey());
      if(defaultSchema == null) defaultSchema = schema;
      root.add(schema);
      schemaNames.add(e.getKey());
    }
    logger.debug("Registered schemas for {}", schemaNames);
    return defaultSchema;
  }
  
  
  private class StorageEngineEntry{
    StorageEngineConfig config;
    SchemaProvider provider;
    
    
    public StorageEngineEntry(StorageEngineConfig config, SchemaProvider provider) {
      super();
      this.config = config;
      this.provider = provider;
    }
    
    public StorageEngineConfig getConfig() {
      return config;
    }
    public SchemaProvider getProvider() {
      return provider;
    }
    
  }
}
