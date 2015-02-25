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
package org.apache.drill.exec.server;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ClusterCoordinator.RegistrationHandle;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.rest.DrillRestServer;
import org.apache.drill.exec.service.ServiceEngine;
import org.apache.drill.exec.store.sys.CachingStoreProvider;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.store.sys.PStoreRegistry;
import org.apache.drill.exec.store.sys.local.LocalPStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.apache.zookeeper.Environment;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.servlet.ServletContainer;

import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;
import com.google.common.io.Closeables;

/**
 * Starts, tracks and stops all the required services for a Drillbit daemon to work.
 */
public class Drillbit implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Drillbit.class);
  static {
    Environment.logEnv("Drillbit environment:.", logger);
  }

  public static Drillbit start(final StartupOptions options) throws DrillbitStartupException {
    return start(DrillConfig.create(options.getConfigLocation()), null);
  }

  public static Drillbit start(final DrillConfig config) throws DrillbitStartupException {
    return start(config, null);
  }

  public static Drillbit start(final DrillConfig config, final RemoteServiceSet remoteServiceSet)
      throws DrillbitStartupException {
    logger.debug("Setting up Drillbit.");
    Drillbit bit;
    try {
      bit = new Drillbit(config, remoteServiceSet);
    } catch (Exception ex) {
      throw new DrillbitStartupException("Failure while initializing values in Drillbit.", ex);
    }

    logger.debug("Starting Drillbit.");
    try {
      bit.run();
    } catch (Exception e) {
      bit.close();
      throw new DrillbitStartupException("Failure during initial startup of Drillbit.", e);
    }
    return bit;
  }

  public static void main(final String[] cli) throws DrillbitStartupException {
    StartupOptions options = StartupOptions.parse(cli);
    start(options);
  }

  private final ClusterCoordinator coord;
  private final ServiceEngine engine;
  private final PStoreProvider storeProvider;
  private final WorkManager manager;
  private final BootStrapContext context;
  private final Server embeddedJetty;
  private RegistrationHandle registrationHandle;

  public Drillbit(DrillConfig config, RemoteServiceSet serviceSet) throws Exception {
    final boolean allowPortHunting = serviceSet != null;
    final boolean enableHttp = config.getBoolean(ExecConstants.HTTP_ENABLE);
    context = new BootStrapContext(config);
    manager = new WorkManager(context);
    engine = new ServiceEngine(manager.getControlMessageHandler(), manager.getUserWorker(), context,
        manager.getWorkBus(), manager.getDataHandler(), allowPortHunting);

    if (enableHttp) {
      embeddedJetty = new Server(config.getInt(ExecConstants.HTTP_PORT));
    } else {
      embeddedJetty = null;
    }

    if (serviceSet != null) {
      coord = serviceSet.getCoordinator();
      storeProvider = new CachingStoreProvider(new LocalPStoreProvider(config));
    } else {
      coord = new ZKClusterCoordinator(config);
      storeProvider = new PStoreRegistry(this.coord, config).newPStoreProvider();
    }
  }

  private void startJetty() throws Exception {
    if (embeddedJetty == null) {
      return;
    }

    final ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setShowMessageInTitle(true);

    final ServletContextHandler servletContextHandler =
        new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContextHandler.setErrorHandler(errorHandler);
    servletContextHandler.setContextPath("/");
    embeddedJetty.setHandler(servletContextHandler);

    final ServletHolder servletHolder = new ServletHolder(new ServletContainer(new DrillRestServer(manager)));
//    servletHolder.setInitParameter(ServerProperties.PROVIDER_PACKAGES, "org.apache.drill.exec.server");
    servletHolder.setInitOrder(1);
    servletContextHandler.addServlet(servletHolder, "/*");

    servletContextHandler.addServlet(
        new ServletHolder(new MetricsServlet(context.getMetrics())), "/status/metrics");
    servletContextHandler.addServlet(new ServletHolder(new ThreadDumpServlet()), "/status/threads");

    final ServletHolder staticHolder = new ServletHolder("static", DefaultServlet.class);
    staticHolder.setInitParameter("resourceBase", Resource.newClassPathResource("/rest/static").toString());
    staticHolder.setInitParameter("dirAllowed","false");
    staticHolder.setInitParameter("pathInfoOnly","true");
    servletContextHandler.addServlet(staticHolder,"/static/*");

    embeddedJetty.start();
  }

  public void run() throws Exception {
    coord.start(10000);
    storeProvider.start();
    final DrillbitEndpoint md = engine.start();
    manager.start(md, engine.getController(), engine.getDataConnectionCreator(), coord, storeProvider);
    final DrillbitContext drillbitContext = manager.getContext();
    drillbitContext.getStorage().init();
    drillbitContext.getOptionManager().init();
    registrationHandle = coord.register(md);
    startJetty();

    Runtime.getRuntime().addShutdownHook(new ShutdownThread());
  }

  @Override
  public void close() {
    if (coord != null && registrationHandle != null) {
      coord.unregister(registrationHandle);
    }

    try {
      Thread.sleep(context.getConfig().getInt(ExecConstants.ZK_REFRESH) * 2);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while sleeping during coordination deregistration.");
    }

    if (embeddedJetty != null) {
      try {
        embeddedJetty.stop();
      } catch (Exception e) {
        logger.warn("Failure while shutting down embedded jetty server.");
      }
    }

    Closeables.closeQuietly(engine);
    AutoCloseables.close(storeProvider, logger);
    Closeables.closeQuietly(coord);
    AutoCloseables.close(manager, logger);
    Closeables.closeQuietly(context);

    logger.info("Shutdown completed.");
  }

  private class ShutdownThread extends Thread {
    ShutdownThread() {
      setName("Drillbit-ShutdownHook");
    }

    @Override
    public void run() {
      logger.info("Received shutdown request.");
      close();
    }
  }

  public DrillbitContext getContext() {
    return manager.getContext();
  }
}
