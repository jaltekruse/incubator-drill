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

/**
 * Run several tpch queries and inject an OutOfMemoryException in ScanBatch that will cause an OUT_OF_MEMORY outcome to
 * be propagated downstream. Make sure the proper "memory error" message is sent to the client.
 */
public class TestAllocationException extends BaseTestQuery {

  private static final String SINGLE_MODE = "ALTER SESSION SET `planner.disable_exchanges` = true";

  private void testWithException(final String fileName) throws Exception {
    testWithException(fileName, OutOfMemoryRuntimeException.class);
  }

  private void testWithException(final String fileName, Class<? extends Throwable> exceptionClass) throws Exception{
    test(SINGLE_MODE);

    CoordinationProtos.DrillbitEndpoint endpoint = bits[0].getContext().getEndpoint();

    String controlsString = "{\"injections\":[{"
      + "\"address\":\"" + endpoint.getAddress() + "\","
      + "\"port\":\"" + endpoint.getUserPort() + "\","
      + "\"type\":\"exception\","
      + "\"siteClass\":\"" + TopLevelAllocator.class.getName() + "\","
      + "\"desc\":\"" + TopLevelAllocator.CHILD_ALLOCATOR_INJECTION_SITE + "\","
      + "\"nSkip\":200,"
      + "\"nFire\":1,"
      + "\"exceptionClass\":\"" + exceptionClass.getName() + "\""
      + "}]}";
    ControlsInjectionUtil.setControls(client, controlsString);

    String query = getFile(fileName);

    try {
      test(query);
    } catch(UserException uex) {
      System.out.println("exception: " + uex.getMessage());
      DrillPBError error = uex.getOrCreatePBError(false);
      Assert.assertEquals(DrillPBError.ErrorType.RESOURCE, error.getErrorType());
      Assert.assertTrue("Error message isn't related to memory error",
        uex.getMessage().contains(UserException.MEMORY_ERROR_MSG));
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
