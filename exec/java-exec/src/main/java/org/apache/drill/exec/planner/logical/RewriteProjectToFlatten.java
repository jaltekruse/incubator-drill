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

import org.apache.drill.exec.planner.physical.DrillFlattenPrel;
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

public class RewriteProjectToFlatten extends RelShuttleImpl {

  RelDataTypeFactory factory;
  DrillOperatorTable table;

  public RewriteProjectToFlatten(RelDataTypeFactory factory, DrillOperatorTable table) {
    super();
    this.factory = factory;
    this.table = table;
  }

  @Override
  public RelNode visit(RelNode node) {

    // The ProjectRel referenced by the RelShuttle is a final class in calcite
    // we are extending from its parent to allow custom Drill functionality
    // but this disables the ability to use the RelShuttle properly
    if (node instanceof ProjectRelBase) {
      ProjectRelBase project = (ProjectRelBase) node;
      List<RexNode> exprList = new ArrayList<>();
      boolean rewrite = false;

      List<RelDataTypeField> relDataTypes = new ArrayList();
      int i = 0;
      RexNode flatttenExpr = null;
      for (RexNode rex : project.getChildExps()) {
        RexNode newExpr = rex;
        if (rex instanceof RexCall) {
          RexCall function = (RexCall) rex;
          String functionName = function.getOperator().getName();
          int nArgs = function.getOperands().size();
          // TODO - determine if I need to care about case sensitivity here

          if (functionName.equalsIgnoreCase("flatten") ) {
            rewrite = true;
            newExpr = flatttenExpr;
            RexBuilder builder = new RexBuilder(factory);
            flatttenExpr = builder.makeInputRef( new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory), i);
          }
        }
        relDataTypes.add(project.getRowType().getFieldList().get(i));
        i++;
        exprList.add(newExpr);
      }
      if (rewrite == true) {
        // TODO - figure out what is the right setting for the traits
        ProjectRelBase newProject = project.copy(project.getTraitSet(), project.getInput(0), exprList, new RelRecordType(relDataTypes));
        DrillFlattenPrel flatten = new DrillFlattenPrel(project.getCluster(), project.getTraitSet(), newProject, flatttenExpr);
        return visitChild(flatten, 0, newProject);
      }

      return visitChild(project, 0, project.getChild());
    } else {
      return super.visit(node);
    }
  }
}
