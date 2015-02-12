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
package org.apache.drill.exec.ops;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

import io.netty.buffer.DrillBuf;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.SimpleOptiqSchema;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.QueryOptionManager;
import org.apache.drill.exec.store.PartitionExplorer;
import org.apache.drill.exec.store.StoragePluginRegistry;

// TODO - consider re-name to PlanningContext, as the query execution context actually appears
// in fragment contexts

// TODO except for a couple of tests, this is only created by Foreman
// TODO the many methods that just return drillbitContext.getXxx() should be replaced with getDrillbitContext()
public class QueryContext implements Closeable, UdfUtilities{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryContext.class);

  private final DrillbitContext drillbitContext;
  private final UserSession session;
  private final OptionManager queryOptions;
  private final PlannerSettings plannerSettings;
  private final DrillOperatorTable table;

  // most of the memory consumed by planning is on-heap, as the calcite planning library
  // represents plans as graphs of POJOs. An allocator is created for the QueryContext (
  // which is used for planning time constant expression evaluation)
  private final BufferAllocator allocator;
  private final BufferManager bufferManager;
  private static final int INITIAL_OFF_HEAP_ALLOCATION = 1024 * 1024;
  private static final int MAX_OFF_HEAP_ALLOCATION = 16 * 1024 * 1024;


  public QueryContext(final UserSession session, final DrillbitContext drllbitContext) {
    super();
    this.drillbitContext = drllbitContext;
    this.session = session;
    this.queryOptions = new QueryOptionManager(session.getOptions());
    this.plannerSettings = new PlannerSettings(queryOptions, getFunctionRegistry());
    plannerSettings.setNumEndPoints(drillbitContext.getBits().size());
    this.table = new DrillOperatorTable(getFunctionRegistry());
    try {
      this.allocator = drllbitContext.getAllocator().getChildAllocator(null, INITIAL_OFF_HEAP_ALLOCATION, MAX_OFF_HEAP_ALLOCATION, false);
    } catch (OutOfMemoryException e) {
      throw new DrillRuntimeException("Error creating off-heap allocator for planning context.",e);
    }
    this.bufferManager = new BufferManager(this.allocator, null);
  }


  public PlannerSettings getPlannerSettings() {
    return plannerSettings;
  }

  public UserSession getSession() {
    return session;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public SchemaPlus getNewDefaultSchema() {
    final SchemaPlus rootSchema = getRootSchema();
    final SchemaPlus defaultSchema = session.getDefaultSchema(rootSchema);
    if (defaultSchema == null) {
      return rootSchema;
    }else{
      return defaultSchema;
    }
  }

  public SchemaPlus getRootSchema() {
    final SchemaPlus rootSchema = SimpleOptiqSchema.createRootSchema(false);
    drillbitContext.getSchemaFactory().registerSchemas(session, rootSchema);
    return rootSchema;
  }

  public OptionManager getOptions() {
    return queryOptions;
  }

  public DrillbitEndpoint getCurrentEndpoint() {
    return drillbitContext.getEndpoint();
  }

  public StoragePluginRegistry getStorage() {
    return drillbitContext.getStorage();
  }

  public Collection<DrillbitEndpoint> getActiveEndpoints() {
    return drillbitContext.getBits();
  }

  public DrillConfig getConfig() {
    return drillbitContext.getConfig();
  }

  public FunctionImplementationRegistry getFunctionRegistry() {
    return drillbitContext.getFunctionImplementationRegistry();
  }

  public DrillOperatorTable getDrillOperatorTable() {
    return table;
  }

  /**
   * TODO - generate the query start time and record the current timezone
   * at the start of planning instead of the start of physical plan materialization.
   *
   * @return
   */
  @Override
  public QueryDateTimeInfo getQueryDateTimeInfo() {
    throw new UnsupportedOperationException("Query start time not currently available during planning.");
  }

  @Override
  public DrillBuf getManagedBuffer() {
    return bufferManager.getManagedBuffer();
  }

  @Override
  public PartitionExplorer getPartitionExplorer() {
    return drillbitContext.getStorage();
  }

  public static int closedAttemptedCount = 0;
  public static int closedFinishedCount = 0;

  @Override
  public void close() throws IOException {
    System.out.println("closes attempted: " + closedAttemptedCount + " closed completed: " + closedFinishedCount );
    closedAttemptedCount++;
    bufferManager.close();
    allocator.close();
    closedFinishedCount++;
    System.out.println("closes attempted: " + closedAttemptedCount + " closed completed: " + closedFinishedCount );
  }

}
