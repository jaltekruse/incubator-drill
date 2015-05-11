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
package org.apache.drill;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.exec.server.options.TypeValidators.BooleanValidator;
import org.apache.drill.exec.server.options.TypeValidators.DoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.LongValidator;
import org.apache.drill.exec.server.options.TypeValidators.StringValidator;

public class OptionTestUtils {

  /**
   * Should be placed at the start of a test that requires disabling exchanges.
   *
   * Keep this in sync with the resetToDefaultExchanges() method.
   * @throws Exception
   */
  public static void forceExchanges(DrillClient client) throws RuntimeException {
    setOption(client, ExecConstants.PLANNER_SLICE_TARGET, 1);
    // TODO - evaluate the benefits of disabling the ExcessiveExchangeIdentifier.removeExcessiveEchanges()
    // call in DefaulSqlHandler.convertToPrel (for tests only) to force exchanges without disabling broadcast
    setOption(client, PlannerSettings.BROADCAST, false);
  }

  /**
   * After a test that requires disabling exchanges has been run, this should be placed
   * in a finally block to ensure that the session options set do not affect other tests.
   *
   * Keep this in sync with the forceExchanges() method.
   * @throws Exception
   */
  public static void resetToDefaultExchanges(DrillClient client) throws RuntimeException {
    resetOption(client, ExecConstants.PLANNER_SLICE_TARGET);
    resetOption(client, PlannerSettings.BROADCAST);
  }

  /**
   * Convenience method for setting a list of session values to true, useful for
   * enabling a series of options that default to 'false' to test uncommon configurations.
   *
   * @param validators - the validators for all of the options to turn on
   * @throws Exception
   */
  public static void setAllTrue(DrillClient client, BooleanValidator... validators) throws RuntimeException {
    for (BooleanValidator validator : validators) {
      setOption(client, validator, true);
    }
  }

  /**
   * Convenience method for setting a list of session values to false, useful for
   * enabling a series of options that default to 'true' to test uncommon configurations.
   *
   * @param validators - the validators for all of the options to turn off
   * @throws Exception
   */
  public static void setAllFalse(DrillClient client, BooleanValidator... validators) throws RuntimeException {
    for (BooleanValidator validator : validators) {
      setOption(client, validator, false);
    }
  }

  public static void setOption(DrillClient client, BooleanValidator validator, boolean value) throws RuntimeException {
    try {
      QueryTestUtil
          .test(client, String.format("ALTER SESSION SET `%s` = %s", validator.name(), (Boolean) value));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void resetOption(DrillClient client,OptionValidator validator) throws RuntimeException {
    try {
      QueryTestUtil
          .test(client,
              String.format("ALTER SESSION SET `%s` = %s", validator.name(), validator.getDefaultString()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void resetOptions(DrillClient client, OptionValidator... validators) throws RuntimeException {
    for (OptionValidator validator : validators) {
      resetOption(client, validator);
    }
  }

  public static void setOption(DrillClient client,DoubleValidator validator, double value) throws RuntimeException {
    try {
      QueryTestUtil.test(client, String.format("ALTER SESSION SET `%s` = %s", validator.name(), (Double) value));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void setOption(DrillClient client, StringValidator validator, String value) throws RuntimeException{
    Preconditions.checkNotNull(value);
    try {
      QueryTestUtil.test(client, String.format("ALTER SESSION SET `%s` = '%s'", validator.name(), value));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void setOption(DrillClient client, LongValidator validator, long value) throws RuntimeException {
    Preconditions.checkNotNull(value);
    try {
      QueryTestUtil.test(client, String.format("ALTER SESSION SET `%s` = %s", validator.name(), (Long) value));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
