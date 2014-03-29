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
package org.apache.drill.exec.store.options;


import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.store.ischema.FixedTable;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.VarCharVector;

public class OptionValueTable extends FixedTable {
  static final String tableName = "OPTIONS";
  static final String[] fieldNames = {"NAME",     "CATEGORY",     "SESSION",    "DEFAULT" };
  static final TypeProtos.MajorType[] fieldTypes= {Types.required(TypeProtos.MinorType.VARCHAR),
    Types.required(TypeProtos.MinorType.VARCHAR),
    Types.optional(TypeProtos.MinorType.VARCHAR),
    Types.required(TypeProtos.MinorType.VARCHAR)};

  static final int NAME = 0;
  static final int CATEGORY = 1;
  static final int SESSION = 2;
  static final int DEFAULT = 3;

  public OptionValueTable() {
    super(tableName, fieldNames, fieldTypes);
  }

  public boolean writeRowToVectors(int index, Object[] row) {
    boolean success =
        setSafe((VarCharVector)vectors.get(NAME), index, (String)row[NAME]) &&
            setSafe((VarCharVector)vectors.get(CATEGORY), index, (String)row[CATEGORY]) &&
            setSafe((VarCharVector)vectors.get(DEFAULT), index, (String)row[DEFAULT]);
    if (row[SESSION] != null ){
      return setSafe((VarCharVector)vectors.get(SESSION), index, (String)row[SESSION]);
    }
    return success;
  }
}
