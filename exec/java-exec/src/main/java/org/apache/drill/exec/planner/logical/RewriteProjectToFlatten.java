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
            System.out.println("expression contains flatten");
            rewrite = true;
//            i++;
//          assert nArgs == 2 && function.getOperands().get(1) instanceof RexLiteral;
            flatttenExpr = function.getOperands().get(0);
            newExpr = flatttenExpr;
//            continue;
//          RexBuilder builder = new RexBuilder(factory);
//
//          // construct the new function name based on the input argument
//          String newFunctionName = functionName + literal;
//
//          // Look up the new function name in the drill operator table
//          List<SqlOperator> operatorList = table.getSqlOperator(newFunctionName);
//          assert operatorList.size() > 0;
//          SqlFunction newFunction = null;
//
//          // Find the SqlFunction with the correct args
//          for (SqlOperator op : operatorList) {
//            if (op.getOperandTypeChecker().getOperandCountRange().isValidCount(nArgs - 1)) {
//              newFunction = (SqlFunction) op;
//              break;
//            }
//          }
//          assert newFunction != null;
//
//          // create the new expression to be used in the rewritten project
//          newExpr = builder.makeCall(newFunction, function.getOperands().subList(0, 1));
//          rewrite = true;
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
