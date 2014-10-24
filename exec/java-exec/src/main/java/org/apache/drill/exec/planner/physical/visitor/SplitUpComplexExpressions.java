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
package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import net.hydromatic.optiq.tools.RelConversionException;

import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.planner.physical.DrillFlattenPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.visitor.BasePrelVisitor;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelShuttleImpl;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypeFieldImpl;
import org.eigenbase.reltype.RelRecordType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.util.NlsString;

import java.util.ArrayList;
import java.util.List;

public class SplitUpComplexExpressions extends BasePrelVisitor<Prel, Object, RelConversionException> {

  RelDataTypeFactory factory;
  DrillOperatorTable table;
  FunctionImplementationRegistry funcReg;

  public SplitUpComplexExpressions(RelDataTypeFactory factory, DrillOperatorTable table, FunctionImplementationRegistry funcReg) {
    super();
    this.factory = factory;
    this.table = table;
    this.funcReg = funcReg;
  }

  @Override
  public Prel visitPrel(Prel prel, Object value) throws RelConversionException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }


  @Override
  public Prel visitProject(ProjectPrel node, Object unused) throws RelConversionException {

    ProjectPrel project = node;
    List<RexNode> exprList = new ArrayList<>();
    boolean rewrite = false;

    List<RelDataTypeField> relDataTypes = new ArrayList();
    int i = 0;
    RexNode flatttenExpr = null;
    RexVisitorComplexExprSplitter exprSplitter = new RexVisitorComplexExprSplitter(factory, funcReg);
    for (RexNode rex : project.getChildExps()) {
      relDataTypes.add(project.getRowType().getFieldList().get(i));
      i++;
      exprList.add(rex.accept(exprSplitter));
    }
    List<RexNode> complexExprs = exprSplitter.getComplexExprs();

    RelNode originalInput = project.getInput(0);
    ProjectPrel childProject;
    
    List<RexNode> allExprs = new ArrayList();
    RexNode currRexNode;
    while (complexExprs.size() > 0) {
      currRexNode = complexExprs.remove(0);
      allExprs.add(currRexNode);
      childProject = new ProjectPrel(node.getCluster(), project.getTraitSet(), originalInput, Lists.newArrayList(currRexNode), new RelRecordType(relDataTypes));
      originalInput = childProject;
    }
//      DrillFlattenPrel flatten = new DrillFlattenPrel(project.getCluster(), project.getTraitSet(), newProject, flatttenExpr);
    return new ProjectPrel(node.getCluster(), project.getTraitSet(), originalInput, exprList, new RelRecordType(relDataTypes));
  }
  
//  ProjectPrel newProject = new ProjectPrel(node.getCluster(), project.getTraitSet(), , exprList, new RelRecordType(relDataTypes));

}
