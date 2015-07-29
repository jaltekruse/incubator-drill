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
package org.apache.drill.exec;

import static org.junit.Assert.assertFalse;

import com.google.common.base.Joiner;
import org.apache.drill.exec.hive.HiveTestBase;
import org.junit.Ignore;
import org.junit.Test;

public class TestHivePartitionPruning extends HiveTestBase {
  //Currently we do not have a good way to test plans so using a crude string comparison
  @Test
  public void testSimplePartitionFilter() throws Exception {
    final String query = "explain plan for select * from hive.`default`.partition_pruning_test where c = 1";
    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse(plan.contains("Filter"));
  }

  @Test
  public void testPartitionFilter() throws Exception {

    String[] partCols = {
        "boolean_part", "tinyint_part", "double_part", "float_part", "int_part",
        "bigint_part", "smallint_part", "string_part"
    };

    String[] regularCols = {
        "boolean_field", "tinyint_field", "double_field", "float_field", "int_field",
        "bigint_field", "smallint_field","string_field"
    };
    String query = "explain plan for select " +
            Joiner.on(", ").join(partCols) + ", " +
            Joiner.on(", ").join(regularCols) + " " +
            "from hive.parquet_text_mixed_fileformat " +
            "where tinyint_part = 64";
    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse(plan.contains("Filter"));
  }

  /* Partition pruning is not supported for disjuncts that do not meet pruning criteria.
   * Will be enabled when we can do wild card comparison for partition pruning
   */
  @Ignore
  public void testDisjunctsPartitionFilter() throws Exception {
    final String query = "explain plan for select * from hive.`default`.partition_pruning_test where (c = 1) or (d = 1)";
    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse(plan.contains("Filter"));
  }

  @Test
  public void testConjunctsPartitionFilter() throws Exception {
    final String query = "explain plan for select * from hive.`default`.partition_pruning_test where c = 1 and d = 1";
    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse(plan.contains("Filter"));
  }

  @Ignore("DRILL-1571")
  public void testComplexFilter() throws Exception {
    final String query = "explain plan for select * from hive.`default`.partition_pruning_test where (c = 1 and d = 1) or (c = 2 and d = 3)";
    final String plan = getPlanInString(query, OPTIQ_FORMAT);

    // Check and make sure that Filter is not present in the plan
    assertFalse(plan.contains("Filter"));
  }
}
