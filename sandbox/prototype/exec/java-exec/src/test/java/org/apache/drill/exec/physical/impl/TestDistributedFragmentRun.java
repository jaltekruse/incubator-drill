/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.physical.impl;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos.QueryType;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;


public class TestDistributedFragmentRun extends PopUnitTestBase{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestDistributedFragmentRun.class);

  
  @Test 
  public void oneBitOneExchangeOneEntryRun() throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile("/physical_single_exchange.json"), Charsets.UTF_8));
      int count = 0;
      for(QueryResultBatch b : results){
        count += b.getHeader().getRowCount();
      }
      assertEquals(100, count);
    }
    

  }


  @Test
  public void oneBitOneExchangeTwoEntryRun() throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile("/physical_single_exchange_double_entry.json"), Charsets.UTF_8));
      int count = 0;
      for(QueryResultBatch b : results){
        count += b.getHeader().getRowCount();
      }
      assertEquals(200, count);
    }


  }

    @Test
    public void oneBitOneExchangeTwoEntryRunLogical() throws Exception{
        RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

        try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
            bit1.run();
            client.connect();
            List<QueryResultBatch> results = client.runQuery(QueryType.LOGICAL, Files.toString(FileUtils.getResourceAsFile("/scan_screen_logical.json"), Charsets.UTF_8));
            int count = 0;
            for(QueryResultBatch b : results){
                count += b.getHeader().getRowCount();
            }
            assertEquals(100, count);
        }


    }

  @Test
  public void testParquetFullEngine() throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    LogicalPlan.parse(DrillConfig.create(),  Files.toString(FileUtils.getResourceAsFile("/parquet_scan_screen.json"), Charsets.UTF_8));

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      List<QueryResultBatch> results = client.runQuery(QueryType.LOGICAL, Files.toString(FileUtils.getResourceAsFile("/parquet_scan_screen.json"), Charsets.UTF_8));
      int count = 0;
      RecordBatchLoader batchLoader = new RecordBatchLoader(bit1.getContext().getAllocator());
      byte[] bytes;
      for(QueryResultBatch b : results){
        count += b.getHeader().getRowCount();
        boolean schemaChanged = batchLoader.load(b.getHeader().getDef(), b.getData());

        int recordCount = 0;
        // print headers.
        if (schemaChanged) {
        } // do not believe any change is needed for when the schema changes, with the current mock scan use case

        for (int i = 1; i < batchLoader.getRecordCount(); i++) {
          recordCount++;
          if (i % 50 == 0){
            for (ValueVector v : batchLoader) {
              System.out.print(v.getField().getName() + " ");

            }
            System.out.println();
          }

          for (ValueVector v : batchLoader) {
             System.out.print(v.getAccessor().getObject(i) + " ");
          }
          System.out.println(

          );
        }
      }
      assertEquals(30000, count);
    }
  }

  @Test
    public void twoBitOneExchangeTwoEntryRun() throws Exception{
      RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

      try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); Drillbit bit2 = new Drillbit(CONFIG, serviceSet); DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());){
        bit1.run();
        bit2.run();
        client.connect();
        List<QueryResultBatch> results = client.runQuery(QueryType.PHYSICAL, Files.toString(FileUtils.getResourceAsFile("/physical_single_exchange_double_entry.json"), Charsets.UTF_8));
        int count = 0;
      for(QueryResultBatch b : results){
        count += b.getHeader().getRowCount();
      }
      assertEquals(200, count);
    }


  }

}
