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
package org.apache.drill.exec.fn.userexceptions;

import com.google.common.collect.ImmutableMap;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.physical.impl.project.ProjectRecordBatch;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataTypeParsingErrorsTest extends BaseTestQuery {

  private static final ImmutableMap<TypeProtos.MinorType, String> EXPECTED_ERRORS =
      ImmutableMap.<TypeProtos.MinorType, String> builder()
      .put(TypeProtos.MinorType.INT, StringFunctionHelpers.NUMBER_FORMAT_ERROR_MSG)
      .put(TypeProtos.MinorType.BIGINT, StringFunctionHelpers.NUMBER_FORMAT_ERROR_MSG)
      .put(TypeProtos.MinorType.DECIMAL18, StringFunctionHelpers.NUMBER_FORMAT_ERROR_MSG)
      .put(TypeProtos.MinorType.DECIMAL9, StringFunctionHelpers.NUMBER_FORMAT_ERROR_MSG)
      .put(TypeProtos.MinorType.DECIMAL28SPARSE, StringFunctionHelpers.NUMBER_FORMAT_ERROR_MSG)
      .put(TypeProtos.MinorType.DECIMAL38SPARSE, StringFunctionHelpers.NUMBER_FORMAT_ERROR_MSG)
      .build();

  public boolean castSupportedForType(TypeProtos.MinorType type) {
    switch (type) {
      case MAP:
      case LIST:
      // we need to define this at the logical expression level, currently literal creation will fail
      case VARBINARY:
      // TODO - this doesn't work in constant folding as calcite does not support interval literals,
      // it may work in regular execution
      case INTERVAL:
        return false;
      default:
        return true;
    }
  }

  @Test
  public void testParsingTypesFromVarcharFailure() throws Exception {
    String invalidData = "abc";
    String invalidDataVarCharFormat = "'" + invalidData + "'";

    // TODO - be sure to run test both on interpreter and regular expression evaluation

    for (TypeProtos.MinorType type : TypeProtos.MinorType.values()) {
      // ignore unused types
      if (Types.UNUSED_TYPES.contains(type)) { continue; }
      // ignore types that cannot be used with cast
      if (! castSupportedForType(type)) { continue; }

      System.out.println(type);
      try {
        // TODO - this is not being evaluated in the constant folding rule, check on this
      test(String.format("select cast(%s as %s) from cp.`store/json/booleanData.json`",
          invalidDataVarCharFormat, Types.getNameOfMinorType(type)));
      } catch (UserRemoteException ex) {
        if (EXPECTED_ERRORS.get(type) != null) {
          System.out.println("=====================================");
          System.out.println("Message from exception: " + ex.getMessage() + "--------");
          // TODO - fix this
          // This only really works if the error messages have the same format specifiers
          // Need to think of a way to keep the messages in sync, or figure out a way to have each
          // type track not only the format string in full, but what order/types the format specifiers
          // for each SQL type's failure message string
          assertTrue("Unexpected error message from cast function evaluation.",
              ex.getMessage().contains(
                  String.format(
                      EXPECTED_ERRORS.get(type),
                      Types.getNameOfMinorType(type),
                      invalidData)));
          // check for context information, currently added to the message when sent back to the client
          assertTrue("Missing expected context information from error message produced by cast function evaluation.",
              ex.getMessage().contains(
                  ProjectRecordBatch.EVALUATION_FAILED_MSG));
        } else {
          ex.printStackTrace();
        }
      }
    }
  }


}
