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
package org.apache.drill.exec.server.options;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.DrillTestWrapper;
import org.apache.drill.TestBuilder;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestOptions extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestOptions.class);

  @Test
  public void testDrillbits() throws Exception{
    test("select * from sys.drillbits;");
  }

  private Object[] getOptionTableRow(OptionValidator validator, String type, String status) {
    return new Object[] {
        validator.name(), validator.getKind().name(), type, status,
        validator.getKind().equals(OptionValue.Kind.LONG) ? validator.getDefault().num_val : null,
        validator.getKind().equals(OptionValue.Kind.STRING) ? validator.getDefault().string_val : null,
        validator.getKind().equals(OptionValue.Kind.BOOLEAN) ? validator.getDefault().bool_val : null,
        validator.getKind().equals(OptionValue.Kind.DOUBLE) ? validator.getDefault().float_val : null
    };
  }

  @Test
  public void testOptionsTable() throws Exception{
    // this is set in the @BeforeClass (setupDefaultTestCluster) method in BaseTestQuery
    resetOption(ExecConstants.MAX_WIDTH_PER_NODE);
    TestBuilder builder = testBuilder()
        .sqlQuery("select * from sys.options")
        // option values come back sorted for ease of reading/manual searching
        .ordered()
        .baselineColumns("name", "kind", "type", "status", "num_val", "string_val", "bool_val", "float_val");

    List<OptionValidator> options = new ArrayList();
    for (OptionValidator validator : SystemOptionManager.VALIDATORS) {
      options.add(validator);
    }
    Collections.sort(options, new Comparator<OptionValidator>() {
      @Override
      public int compare(OptionValidator o1, OptionValidator o2) {
        return o1.name().compareTo(o2.name());
      }
    });

    for (OptionValidator validator : options) {
      // The drillbit controls option is designed to be short lived, so it is only allowed to be set
      // at the session level, it is the only option with this property currently
      if (validator == ExecConstants.DRILLBIT_CONTROLS_VALIDATOR) {
        builder.baselineValues(getOptionTableRow(validator, "SESSION", "DEFAULT"));
      } else {
        builder.baselineValues(getOptionTableRow(validator, "SYSTEM", "DEFAULT"));
      }
      // Session options do not leave the list even if they are set back to the system default.
      // Add an option value here for the MAX_WIDTH_PER_NODE as it is set in all of the tests.
      // To remove the need to match the value set in the tests, in case it changes in the
      // future, I just set the session value back to its default at the top of this test method
      if (validator == ExecConstants.MAX_WIDTH_PER_NODE) {
        builder.baselineValues(getOptionTableRow(validator, "SESSION", "CHANGED"));
      }
    }
    builder.go();

  }

  @Test
  public void testSettingOptions() throws Exception{
    // There is a convenience method for setting session options, these should be used to
    // set custom options for a test in just about every test scenario.
    // To avoid misuse, I am putting the raw query here to set a system option, rather than
    // create a corresponding convenience method, as it will not have many uses if it was provided.
    test(String.format("ALTER SYSTEM set `%s` = true", PlannerSettings.DISABLE_EXCHANGE.name()));
    try {
      testBuilder()
          .sqlQuery(String.format("select bool_val from sys.options where " +
              "name = '%s' and type = 'SYSTEM'", PlannerSettings.DISABLE_EXCHANGE.name()))
          .ordered()
          .baselineColumns("bool_val")
          .baselineValues(true)
          .go();

      try {
        testBuilder()
            .sqlQuery(String.format("select bool_val from sys.options where " +
                "name = '%s' and type = 'SESSION'", PlannerSettings.DISABLE_EXCHANGE.name()))
            .ordered()
            .baselineColumns("bool_val")
            .baselineValues(true)
            .go();
        throw new Exception("Query above should have failed");
      } catch (AssertionError ex) {
        assertTrue("Query failed with wrong error case.", ex.getMessage().contains(DrillTestWrapper.INCORRECT_ROW_COUNT));
      }

      test(String.format("ALTER SESSION set `%s` = true", PlannerSettings.DISABLE_EXCHANGE.name()));

      testBuilder()
          .sqlQuery(String.format("select bool_val from sys.options where " +
              "name = '%s' and type = 'SESSION'", PlannerSettings.DISABLE_EXCHANGE.name()))
          .ordered()
          .baselineColumns("bool_val")
          .baselineValues(true)
          .go();
    } finally {
      resetOption(PlannerSettings.DISABLE_EXCHANGE);
      test(String.format("ALTER SYSTEM set `%s` = false", PlannerSettings.DISABLE_EXCHANGE.name()));
    }
  }
}
