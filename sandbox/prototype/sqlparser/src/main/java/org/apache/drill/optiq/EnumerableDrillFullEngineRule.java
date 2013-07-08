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
package org.apache.drill.optiq;

import net.hydromatic.optiq.rules.java.EnumerableConvention;
import org.apache.drill.optiq.ref.DrillRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRule;

/**
 * Rule that converts any Drill relational expression to enumerable format by
 * adding a {@link org.apache.drill.optiq.ref.EnumerableDrillRel}.
 */
public class EnumerableDrillFullEngineRule extends ConverterRule {
  public static final EnumerableDrillFullEngineRule ARRAY_INSTANCE =
      new EnumerableDrillFullEngineRule(EnumerableConvention.ARRAY);
  public static final EnumerableDrillFullEngineRule CUSTOM_INSTANCE =
      new EnumerableDrillFullEngineRule(EnumerableConvention.CUSTOM);

  private EnumerableDrillFullEngineRule(EnumerableConvention outConvention) {
    super(RelNode.class,
        DrillRel.CONVENTION,
        outConvention,
        "EnumerableDrillRule." + outConvention);
  }

  @Override
  public boolean isGuaranteed() {
    return true;
  }

  @Override
  public RelNode convert(RelNode rel) {
    assert rel.getTraitSet().contains(DrillRel.CONVENTION);
    return new EnumerableDrillFullEngineRel(rel.getCluster(),
        rel.getTraitSet().replace(getOutConvention()),
        rel);
  }
}

// End EnumerableDrillRule.java
