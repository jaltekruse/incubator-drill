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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import net.hydromatic.linq4j.expressions.DefaultExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.Frameworks;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.logical.StorageEngines;
import org.apache.drill.exec.rpc.user.DrillUser;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.ischema.InfoSchemaConfig;
import org.apache.drill.exec.store.ischema.InfoSchemaStoragePlugin;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.apache.drill.exec.store.options.OptionValueStorageConfig;
import org.apache.drill.exec.store.options.OptionValueStoragePlugin;


public class StoragePluginRegistry implements Iterable<Map.Entry<String, StoragePlugin>>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginRegistry.class);

  private Map<Object, Constructor<? extends StoragePlugin>> availableEngines = new HashMap<Object, Constructor<? extends StoragePlugin>>();
  private final ImmutableMap<String, StoragePlugin> engines;

  private DrillbitContext context;
  private final DrillSchemaFactory schemaFactory = new DrillSchemaFactory();

  private static final Expression EXPRESSION = new DefaultExpression(Object.class);

  public StoragePluginRegistry(DrillbitContext context) {
    try{
    this.context = context;
    init(context.getConfig());
    this.engines = ImmutableMap.copyOf(createEngines());
    }catch(RuntimeException e){
      logger.error("Failure while loading storage engine registry.", e);
      throw new RuntimeException("Faiure while reading and loading storage plugin configuration.", e);
    }
  }

  @SuppressWarnings("unchecked")
  public void init(DrillConfig config){
    Collection<Class<? extends StoragePlugin>> engines = PathScanner.scanForImplementations(StoragePlugin.class, config.getStringList(ExecConstants.STORAGE_ENGINE_SCAN_PACKAGES));
    logger.debug("Loading storage engines {}", engines);
    for(Class<? extends StoragePlugin> engine: engines){
      int i =0;
      for(Constructor<?> c : engine.getConstructors()){
        Class<?>[] params = c.getParameterTypes();
        if(params.length != 3 || params[1] != DrillbitContext.class || !StoragePluginConfig.class.isAssignableFrom(params[0]) || params[2] != String.class){
          logger.info("Skipping StorageEngine constructor {} for engine class {} since it doesn't implement a [constructor(StorageEngineConfig, DrillbitContext, String)]", c, engine);
          continue;
        }
        availableEngines.put(params[0], (Constructor<? extends StoragePlugin>) c);
        i++;
      }
      if(i == 0){
        logger.debug("Skipping registration of StorageEngine {} as it doesn't have a constructor with the parameters of (StorangeEngineConfig, Config)", engine.getCanonicalName());
      }
    }


  }

  private Map<String, StoragePlugin> createEngines(){
    StorageEngines engines = null;
    Map<String, StoragePlugin> activeEngines = new HashMap<String, StoragePlugin>();
    try{
      String enginesData = Resources.toString(Resources.getResource("storage-engines.json"), Charsets.UTF_8);
      engines = context.getConfig().getMapper().readValue(enginesData, StorageEngines.class);
    }catch(IOException e){
      throw new IllegalStateException("Failure while reading storage engines data.", e);
    }

    for(Map.Entry<String, StoragePluginConfig> config : engines){
      try{
        StoragePlugin plugin = create(config.getKey(), config.getValue());
        activeEngines.put(config.getKey(), plugin);
      }catch(ExecutionSetupException e){
        logger.error("Failure while setting up StoragePlugin with name: '{}'.", config.getKey(), e);
      }
    }
    activeEngines.put("INFORMATION_SCHEMA", new InfoSchemaStoragePlugin(new InfoSchemaConfig(), context, "INFORMATION_SCHEMA"));
    activeEngines.put("OPTIONS", new OptionValueStoragePlugin(new OptionValueStorageConfig(), context, "OPTIONS"));

    return activeEngines;
  }

  public StoragePlugin getEngine(String registeredStorageEngineName) throws ExecutionSetupException {
    return engines.get(registeredStorageEngineName);
  }

  public StoragePlugin getEngine(StoragePluginConfig config) throws ExecutionSetupException {
    if(config instanceof NamedStoragePluginConfig){
      return engines.get(((NamedStoragePluginConfig) config).name);
    }else{
      // TODO: for now, we'll throw away transient configs.  we really ought to clean these up.
      return create(null, config);
    }
  }

  public FormatPlugin getFormatPlugin(StoragePluginConfig storageConfig, FormatPluginConfig formatConfig) throws ExecutionSetupException{
    StoragePlugin p = getEngine(storageConfig);
    if(!(p instanceof FileSystemPlugin)) throw new ExecutionSetupException(String.format("You tried to request a format plugin for a stroage engine that wasn't of type FileSystemPlugin.  The actual type of plugin was %s.", p.getClass().getName()));
    FileSystemPlugin storage = (FileSystemPlugin) p;
    return storage.getFormatPlugin(formatConfig);
  }

  private StoragePlugin create(String name, StoragePluginConfig engineConfig) throws ExecutionSetupException {
    StoragePlugin engine = null;
    Constructor<? extends StoragePlugin> c = availableEngines.get(engineConfig.getClass());
    if (c == null)
      throw new ExecutionSetupException(String.format("Failure finding StorageEngine constructor for config %s",
          engineConfig));
    try {
      engine = c.newInstance(engineConfig, context, name);
      return engine;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException() : e;
      if (t instanceof ExecutionSetupException)
        throw ((ExecutionSetupException) t);
      throw new ExecutionSetupException(String.format(
          "Failure setting up new storage engine configuration for config %s", engineConfig), t);
    }
  }

  @Override
  public Iterator<Entry<String, StoragePlugin>> iterator() {
    return engines.entrySet().iterator();
  }

  public DrillSchemaFactory getSchemaFactory(){
    return schemaFactory;
  }

  public class DrillSchemaFactory implements SchemaFactory{

    @Override
    public void registerSchemas(DrillUser user, SchemaPlus parent) {
      for(Map.Entry<String, StoragePlugin> e : engines.entrySet()){
        e.getValue().registerSchemas(user, parent);
      }
    }

  }

}
