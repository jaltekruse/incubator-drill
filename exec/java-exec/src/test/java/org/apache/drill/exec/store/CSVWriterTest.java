package org.apache.drill.exec.store;

import com.google.common.io.Files;
import org.apache.commons.codec.Charsets;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

public class CSVWriterTest {

  @Test
  public void testCSVWriter() throws Exception {

    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();
    DrillConfig config = DrillConfig.create();
    try(Drillbit bit1 = new Drillbit(config, serviceSet); DrillClient client = new DrillClient(config, serviceSet.getCoordinator())) {
      bit1.run();
      client.connect();
      List<QueryResultBatch> results;
      results = client.runQuery(UserProtos.QueryType.LOGICAL,
          Files.toString(FileUtils.getResourceAsFile("/store/cvs_first_logical_plan.json"), Charsets.UTF_8));

      RecordBatchLoader batchLoader = new RecordBatchLoader(bit1.getContext().getAllocator());
      for (QueryResultBatch b : results){
        batchLoader.load(b.getHeader().getDef(), b.getData());
        b.toString();
      }
    }
  }
}
