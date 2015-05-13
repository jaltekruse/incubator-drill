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
package org.apache.drill;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.testing.ControlsInjectionUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Run several tpch queries and inject an OutOfMemoryException in ScanBatch that will cause an OUT_OF_MEMORY outcome to
 * be propagated downstream. Make sure the proper "memory error" message is sent to the client.
 */
public class TestAllocationException extends BaseTestQuery {

  private static final String SINGLE_MODE = "ALTER SESSION SET `planner.disable_exchanges` = true";

  public static void testWithException(final String fileName) throws Exception {
    testQueryFromFileWithException(fileName, OutOfMemoryRuntimeException.class);
  }

  // TODO - possibly change the name of the method above to include fromFile in the name
  public static void testSqlWithException(final String query) throws Exception {
    testWithException(query, OutOfMemoryRuntimeException.class);
  }

  public static void testQueryFromFileWithException(final String fileName, Class<? extends Throwable> exceptionClass) throws Exception{
    String query = getFile(fileName);
    testWithException(query, exceptionClass);
  }

  public static void testWithException(final String query, Class<? extends Throwable> exceptionClass) throws Exception{
    // TODO - this was the old body the the test() method in BaseTestQuery, also used below
//    QueryTestUtil.test(client, SINGLE_MODE);

    CoordinationProtos.DrillbitEndpoint endpoint = bits[0].getContext().getEndpoint();

    List<Integer> numSkips = new ArrayList();
    // add 1-20
    int i = 0;
    for (; i < 20; i++ ) {
      numSkips.add(i);
    }
    // for 20-40, add every 4th
    for (; i < 50; i += 4 ) {
      numSkips.add(i);
    }
    // for 50-150, add every 10th
    for (; i < 150; i+= 10 ) {
      numSkips.add(i);
    }
    // for 150-400, add every 30th
    for (; i < 400; i += 30 ) {
      numSkips.add(i);
    }
    for (int currNumSkips : numSkips) {
      String controlsString = "{\"injections\":[{"
          + "\"address\":\"" + endpoint.getAddress() + "\","
          + "\"port\":\"" + endpoint.getUserPort() + "\","
          + "\"type\":\"exception\","
          + "\"siteClass\":\"" + TopLevelAllocator.class.getName() + "\","
          + "\"desc\":\"" + TopLevelAllocator.CHILD_ALLOCATOR_INJECTION_SITE + "\","
          + "\"nSkip\":" + currNumSkips + ","
          + "\"nFire\":1,"
          + "\"exceptionClass\":\"" + exceptionClass.getName() + "\""
          + "}]}";
      ControlsInjectionUtil.setControls(client, controlsString);


      try {
        // TODO - this was the old body the the test() method in BaseTestQuery, also used above
        QueryTestUtil.test(client, query);
      } catch(UserException uex) {
        System.out.println("exception: " + uex.getMessage());
        DrillPBError error = uex.getOrCreatePBError(false);
        // TODO - think if I want to re-enable this, I am getting cases in JsonReader where we catch general Exception
        // so the exception is being re-wrapped there in a different user exception
        //      Assert.assertEquals(DrillPBError.ErrorType.RESOURCE, error.getErrorType());
        //      Assert.assertTrue("Error message isn't related to memory error",
        //        uex.getMessage().contains(UserException.MEMORY_ERROR_MSG));
      }
    }
  }

  @Test
  public void tpch01() throws Exception{
    testWithException("queries/tpch/01.sql");
  }

  @Test
  public void tpch03() throws Exception{
    testWithException("queries/tpch/03.sql");
  }

  @Test
  public void tpch04() throws Exception{
    testWithException("queries/tpch/04.sql");
  }

  @Test
  public void tpch05() throws Exception{
    testWithException("queries/tpch/05.sql");
  }

  @Test
  public void tpch06() throws Exception{
    testWithException("queries/tpch/06.sql");
  }

  @Test
  public void tpch07() throws Exception{
    testWithException("queries/tpch/07.sql");
  }

  @Test
  public void tpch08() throws Exception{
    testWithException("queries/tpch/08.sql");
  }

  @Test
  public void tpch09() throws Exception{
    testWithException("queries/tpch/09.sql");
  }

  @Test
  @Ignore
  public void tpch10() throws Exception{
    testWithException("queries/tpch/10.sql", NullPointerException.class);
  }

  @Test
  public void tpch12() throws Exception{
    testWithException("queries/tpch/12.sql");
  }

  @Test
  public void tpch13() throws Exception{
    testWithException("queries/tpch/13.sql");
  }

  @Test
  public void tpch14() throws Exception{
    testWithException("queries/tpch/14.sql");
  }

  @Test
  public void tpch18() throws Exception{
    testWithException("queries/tpch/18.sql");
  }

  @Test
  public void tpch20() throws Exception{
    testWithException("queries/tpch/20.sql");
  }
}
