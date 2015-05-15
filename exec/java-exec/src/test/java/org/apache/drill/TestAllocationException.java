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
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.testing.ControlsInjectionUtil;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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

    CoordinationProtos.DrillbitEndpoint endpoint = bits[0].getContext().getEndpoint();

    List<Integer> numSkips = new ArrayList();
    numSkips.add(1);
    /*
    // add 1-20
    int i = 0;
    for (; i < 20; i++ ) {
      numSkips.add(i);
    }
    // for 20-50, add every 4th
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
     */
    for (int currNumSkips : numSkips) {
      String controlsString = "{\"injections\":[{"
          + "\"address\":\"" + endpoint.getAddress() + "\","
          + "\"port\":\"" + endpoint.getUserPort() + "\","
          + "\"type\":\"exception\","
          + "\"siteClass\":\"" + TopLevelAllocator.class.getName() + "\","
          + "\"desc\":\"" + TopLevelAllocator.CHILD_BUFFER_INJECTION_SITE + "\","
          + "\"nSkip\":" + currNumSkips + ","
          + "\"nFire\":1,"
          + "\"exceptionClass\":\"" + exceptionClass.getName() + "\""
          + "}]}";
      ControlsInjectionUtil.setControls(client, controlsString);

      long before = 0;
      try {
        // TODO - this was the old body the the test() method in BaseTestQuery

        before = countAllocatedMemory();
        QueryTestUtil.test(client, query);
        // The finally below will still be run to check to make sure we aren't leaking memory even if the
        // test ran successfully
        // in general this break will save us the time of running the query again once we have gone beyond the number
        // of allocations required by the entire query
        break;
      } catch(UserException uex) {
        System.out.println("exception: " + uex.getMessage());
        DrillPBError error = uex.getOrCreatePBError(false);
        // TODO - think if I want to re-enable this, I am getting cases in JsonReader where we catch general Exception
        // so the exception is being re-wrapped there in a different user exception
        //      Assert.assertEquals(DrillPBError.ErrorType.RESOURCE, error.getErrorType());
        //      Assert.assertTrue("Error message isn't related to memory error",
        //        uex.getMessage().contains(UserException.MEMORY_ERROR_MSG));
      } finally {
        long after = countAllocatedMemory();
        assertEquals(String.format("With a value of %d for nSkips - We are leaking %d bytes", currNumSkips, after - before), before, after);
      }
    }
  }

  private static long countAllocatedMemory() {
    // wait to make sure all fragments finished cleaning up
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // just ignore
    }

    long allocated = 0;
    for (Drillbit bit: bits) {
      allocated += bit.getContext().getAllocator().getAllocatedMemory();
    }

    return allocated;
  }

  @Test
  public void testWithNull() throws Exception{
    test(SINGLE_MODE);
    testWithException("queries/tpch/01.sql");
  }

  @Test
  public void testWithOOM() throws Exception{
    test(SINGLE_MODE);
    testWithException("queries/tpch/03.sql", NullPointerException.class);
  }
}
