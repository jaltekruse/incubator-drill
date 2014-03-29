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
package org.apache.drill.exec.server;

import org.apache.drill.exec.DrillSystemTestBase;
import org.apache.drill.exec.client.DrillClient;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

// Ignored to prevent long startup time for ZK
@Ignore
public class TestOptionsZK extends DrillSystemTestBase {

  @BeforeClass
  public static void setUp() throws Exception {
    DrillSystemTestBase.setUp();
  }

  @After
  public void tearDown() {
    stopCluster();
    stopZookeeper();
  }

  @Test
  public void testSubmitPlanSingleNode() throws Exception {
    startZookeeper(1);
    startCluster(1);
    DrillClient client = new DrillClient();
    DrillClient client2 = new DrillClient();
    TestOptions.runOptionTests(client, client2);
    client.close();
    client2.close();
  }
}
