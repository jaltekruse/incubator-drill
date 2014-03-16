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
package org.apache.drill.exec.planner.logical;

import com.google.common.collect.Lists;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.reltype.RelDataType;

import java.util.List;

public class DrillScreenRelOptTable extends DrillRelOptTable {

  public static final double DEFAULT_ROW_COUNT = 5000;

  public DrillScreenRelOptTable(RelOptSchema relOptSchema, List<RelCollation> relCollations) {
    super(Lists.newArrayList("screen"), DEFAULT_ROW_COUNT, new RelDataTypeDrillImpl(new JavaTypeFactoryImpl()),
        relOptSchema, relCollations);
  }
}
