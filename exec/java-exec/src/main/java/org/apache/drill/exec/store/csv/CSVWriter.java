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
package org.apache.drill.exec.store.csv;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.Store;
import org.apache.drill.exec.physical.impl.RootCreator;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.physical.impl.materialize.RecordMaterializer;
import org.apache.drill.exec.physical.impl.materialize.VectorRecordMaterializer;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.proto.UserProtos.QueryResult;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.store.ClassPathFileSystem;
import org.apache.drill.exec.work.foreman.ErrorHelper;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class CSVWriter implements RootCreator<Store> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CSVWriter.class);


 public static final String HADOOP_DEFAULT_NAME = "fs.default.name";
  final CSVStorageEngineConfig configuration;
  final FileSystem fs;
  final Configuration conf;

  public CSVWriter(CSVStorageEngineConfig configuration, DrillConfig config){
    this.configuration = configuration;
    try {
      this.conf = new Configuration();
      this.conf.set("fs.classpath.impl", ClassPathFileSystem.class.getName());
      this.conf.set(HADOOP_DEFAULT_NAME, configuration.getDfsName());
      logger.debug("{}: {}",HADOOP_DEFAULT_NAME, configuration.getDfsName());
      this.fs = FileSystem.get(conf);
    } catch (IOException ie) {
      throw new RuntimeException("Error setting up filesystem", ie);
    }
  }

  @Override
  public RootExec getRoot(FragmentContext context, Store config, List<RecordBatch> children) {
    Preconditions.checkNotNull(children);
    Preconditions.checkArgument(children.size() == 1);
    return new StoreRoot(context, children.iterator().next());
  }


  static class StoreRoot implements RootExec{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoreRoot.class);
    volatile boolean ok = true;

    final RecordBatch incoming;
    final FragmentContext context;
    final UserClientConnection connection;
    private RecordMaterializer materializer;

    public StoreRoot(FragmentContext context, RecordBatch incoming){
      assert context.getConnection() != null : "A screen root should only be run on the driving node which is connected directly to the client.  As such, this should always be true.";

      this.context = context;
      this.incoming = incoming;
      this.connection = context.getConnection();
    }

    @Override
    public boolean next() {
      if(!ok){
        stop();
        return false;
      }

      IterOutcome outcome = incoming.next();
      logger.debug("Screen Outcome {}", outcome);
      switch(outcome){
        case STOP: {
          QueryResult header = QueryResult.newBuilder() //
              .setQueryId(context.getHandle().getQueryId()) //
              .setRowCount(0) //
              .addError(ErrorHelper.logAndConvertError(context.getIdentity(), "Screen received stop request sent.", context.getFailureCause(), logger))
              .setDef(RecordBatchDef.getDefaultInstance()) //
              .setIsLastChunk(true) //
              .build();
          QueryWritableBatch batch = new QueryWritableBatch(header);
          connection.sendResult(listener, batch);

          return false;
        }
        case NONE: {
          if(materializer == null){
            // receive no results.
            context.batchesCompleted.inc(1);
            context.recordsCompleted.inc(incoming.getRecordCount());
            QueryResult header = QueryResult.newBuilder() //
                .setQueryId(context.getHandle().getQueryId()) //
                .setRowCount(0) //
                .setDef(RecordBatchDef.getDefaultInstance()) //
                .setIsLastChunk(true) //
                .build();
            QueryWritableBatch batch = new QueryWritableBatch(header);
            connection.sendResult(listener, batch);

          }else{
            QueryWritableBatch batch = materializer.convertNext(true);
            connection.sendResult(listener, batch);
          }

          return false;
        }
        case OK_NEW_SCHEMA:
          materializer = new VectorRecordMaterializer(context, incoming);
          // fall through.
        case OK:
          context.batchesCompleted.inc(1);
          context.recordsCompleted.inc(incoming.getRecordCount());
          QueryWritableBatch batch = materializer.convertNext(false);
          connection.sendResult(listener, batch);
          return true;
        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    public void stop() {
      incoming.kill();
    }

    private SendListener listener = new SendListener();

    private class SendListener extends BaseRpcOutcomeListener<Ack>{



      @Override
      public void failed(RpcException ex) {
        logger.error("Failure while sending data to user.", ex);
        ErrorHelper.logAndConvertError(context.getIdentity(), "Failure while sending fragment to client.", ex, logger);
        ok = false;
      }

    }

    RecordBatch getIncoming() {
      return incoming;
    }


  }


}
