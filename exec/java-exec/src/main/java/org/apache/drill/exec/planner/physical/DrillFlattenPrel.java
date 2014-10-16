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
package org.apache.drill.exec.planner.physical;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.FlattenPOP;
import org.apache.drill.exec.planner.common.DrillFilterRelBase;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema;

import java.io.IOException;
import java.util.Iterator;

public class DrillFlattenPrel extends DrillFilterRelBase implements Prel {

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getChild());
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    FlattenPop f = new FlattenPOP();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return new BatchSchema.SelectionVectorMode[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
