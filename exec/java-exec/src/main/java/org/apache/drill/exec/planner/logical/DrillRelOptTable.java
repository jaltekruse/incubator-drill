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

import net.hydromatic.linq4j.expressions.Expression;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class DrillRelOptTable implements RelOptTable {

  List<String> qualifiedName;
  double rowCount;
  RelDataType rowType;
  RelOptSchema relOptSchema;
  List<RelCollation> relCollations;

  public DrillRelOptTable(List<String> qualifiedName, double rowCount, RelDataType rowType, RelOptSchema relOptSchema,
                          List<RelCollation> relCollations){
    this.qualifiedName = qualifiedName;
    this.rowCount = rowCount;
    this.rowType = rowType;
    this.relOptSchema = relOptSchema;
    this.relCollations = relCollations;
  }

  @Override
  public List<String> getQualifiedName() {
    return qualifiedName;
  }

  @Override
  public double getRowCount() {
    return rowCount;
  }

  @Override
  public RelDataType getRowType() {
    return rowType;
  }

  @Override
  public RelOptSchema getRelOptSchema() {
    return relOptSchema;
  }

  @Override
  public RelNode toRel(ToRelContext context) {
    return null;
  }

  // TODO - look at docs to see how to fill this in correctly
  @Override
  public List<RelCollation> getCollationList() {
    return relCollations;
  }

  // TODO - look at docs to see how to fill this in correctly
  @Override
  public boolean isKey(BitSet columns) {
    return false;
  }

  // TODO - look at docs to see how to fill this in correctly
  @Override
  public <T> T unwrap(Class<T> clazz) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  // TODO - look at docs to see how to fill this in correctly
  @Override
  public Expression getExpression(Class clazz) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
