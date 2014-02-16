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

import com.google.common.io.Files;
import org.apache.commons.codec.Charsets;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestOptions extends PopUnitTestBase {

  @Test
  public void testSessionOptionSet() throws Exception{
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try(Drillbit bit1 = new Drillbit(CONFIG, serviceSet); DrillClient client =
        new DrillClient(CONFIG, serviceSet.getCoordinator());){
      bit1.run();
      client.connect();
      runQuery("/server/options_set.json", client, bit1);
      runQuery("/server/options_session_check.json", client, bit1);
    }


  }

  public void runQuery(String planLocation, DrillClient client, Drillbit bit1) throws IOException, SchemaChangeException {
    List<QueryResultBatch> results = client.runQuery(UserProtos.QueryType.PHYSICAL,
        Files.toString(FileUtils.getResourceAsFile(planLocation), Charsets.UTF_8));
    int count = 0;
    RecordBatchLoader batchLoader = new RecordBatchLoader(bit1.getContext().getAllocator());
    for(QueryResultBatch b : results){
      boolean schemaChanged = batchLoader.load(b.getHeader().getDef(), b.getData());
      count += b.getHeader().getRowCount();
      for (VectorWrapper vw : batchLoader) {
        ValueVector vv = vw.getValueVector();
        assertTrue(new String("physical").equalsIgnoreCase(
            new String((byte[])vv.getAccessor().getObject(0),Charset.forName("UTF-8"))));
      }
    }
    assertEquals(1, count);

  }
}
