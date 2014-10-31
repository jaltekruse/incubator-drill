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
package org.apache.drill.exec.physical.impl.flatten;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestFlatten extends BaseTestQuery {

  @Test
  public void testFlattenRepeatedList() throws Exception {
    testPhysicalFromFile("flatten/repeated_list_corrected_physical.json");
//    test("select `integer`, `float`, x, flatten(rl) from cp.`/jsoninput/input2_modified.json`");
  }

  @Test
  public void test_complexExprSplitter() throws Exception {
    test("select sum(t.field_1.`value`) from (select flatten(kvgen(convert_fromJSON(map_field))) as field_1 from " +
        "cp.`jsoninput/complex_expr_split_data.json`) as t where t.field_1.key = 'field_2'");
//    test("select flatten(kvgen(convert_fromJSON(map_field))) as field_1 from cp.`jsoninput/complex_expr_split_data.json`");
  }

}
