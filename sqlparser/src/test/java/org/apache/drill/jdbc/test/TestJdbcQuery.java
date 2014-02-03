package org.apache.drill.jdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.sjdbc.GlobalServiceSetReference;
import org.junit.Test;

import com.google.common.base.Stopwatch;

public class TestJdbcQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJdbcQuery.class);

  @Test
  public void doDrillQuery() throws Exception {

    DrillConfig config = DrillConfig.create();
    RemoteServiceSet local = RemoteServiceSet.getLocalServiceSet();

    try (Drillbit bit = new Drillbit(config, local);) {
      bit.run();
      GlobalServiceSetReference.SETS.set(local);

      Class.forName("org.apache.drill.sjdbc.DrillJdbcDriver");
      Properties p = new Properties();
      p.setProperty("zk", "local");

      try (Connection c = DriverManager.getConnection("jdbc:drill:", p);) {
        for(int x =0; x < 1; x++){
          Stopwatch watch = new Stopwatch().start(); 
          Statement s = c.createStatement();
//          ResultSet r = s.executeQuery("select cast(_MAP['RCOMMENT'] as varchar) from \"parquet\".\"/Users/jnadeau/region.parquet\"");
          ResultSet r = s.executeQuery("select R_REGIONKEY, cast(R_NAME as varchar(255)) as region, cast(R_COMMENT as varchar(255)) as comment from parquet.`/Users/jnadeau/region.parquet`");
          boolean first = true;
          while (r.next()) {
            ResultSetMetaData md = r.getMetaData();
            if (first == true) {
              for (int i = 0; i < md.getColumnCount(); i++) {
                System.out.print(md.getColumnName(i));
                System.out.print('\t');
              }
              System.out.println();
              first = false;
            }

           
            for (int i = 0; i < md.getColumnCount(); i++) {
              System.out.print(r.getObject(i));
              System.out.print('\t');
            }
            System.out.println();
          }
          
          System.out.println(String.format("Query completed in %d millis.", watch.elapsed(TimeUnit.MILLISECONDS)));
        }

      }
    }

  }
}
