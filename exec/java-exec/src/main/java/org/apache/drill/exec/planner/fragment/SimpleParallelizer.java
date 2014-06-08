/**
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
 */
package org.apache.drill.exec.planner.fragment;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Materializer.IndexedFragmentNode;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.work.QueryWorkUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * The simple parallelizer determines the level of parallelization of a plan based on the cost of the underlying
 * operations.  It doesn't take into account system load or other factors.  Based on the cost of the query, the
 * parallelization for each major fragment will be determined.  Once the amount of parallelization is done, assignment
 * is done based on round robin assignment ordered by operator affinity (locality) to available execution Drillbits.
 */
public class SimpleParallelizer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleParallelizer.class);
  private final Materializer materializer = new Materializer();

  /**
   * The maximum level or parallelization any stage of the query can do. Note that while this
   * might be the number of active Drillbits, realistically, this could be well beyond that
   * number of we want to do things like speed results return.
   */
  private int globalMaxWidth;
  public SimpleParallelizer setGlobalMaxWidth(int globalMaxWidth) {
    this.globalMaxWidth = globalMaxWidth;
    return this;
  }

  /**
   * Limits the maximum level of parallelization to this factor time the number of Drillbits
   */
  private int maxWidthPerEndpoint;
  public SimpleParallelizer setMaxWidthPerEndpoint(int maxWidthPerEndpoint) {
    this.maxWidthPerEndpoint = maxWidthPerEndpoint;
    return this;
  }


  /**
   * Factor by which a node with endpoint affinity will be favored while creating assignment
   */
  private double affinityFactor = 1.2f;
  public SimpleParallelizer setAffinityFactor(double affinityFactor) {
    this.affinityFactor = affinityFactor;
    return this;
  }

  /**
   * Generate a set of assigned fragments based on the provided planningSet. Do not allow parallelization stages to go
   * beyond the global max width.
   *
   * @param foremanNode     The driving/foreman node for this query.  (this node)
   * @param queryId         The queryId for this query.
   * @param activeEndpoints The list of endpoints to consider for inclusion in planning this query.
   * @param reader          Tool used to read JSON plans
   * @param rootNode        The root node of the PhysicalPlan that we will parallelizing.
   * @param planningSet     The set of queries with collected statistics that we'll work with.
   * @return The list of generated PlanFragment protobuf objects to be assigned out to the individual nodes.
   * @throws ExecutionSetupException
   */
  public QueryWorkUnit getFragments(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId, Collection<DrillbitEndpoint> activeEndpoints,
      PhysicalPlanReader reader, Fragment rootNode, PlanningSet planningSet) throws ExecutionSetupException {
    assignEndpoints(activeEndpoints, planningSet);
    return generateWorkUnit(options, foremanNode, queryId, reader, rootNode, planningSet);
  }

  private QueryWorkUnit generateWorkUnit(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId, PhysicalPlanReader reader, Fragment rootNode,
                                         PlanningSet planningSet) throws ExecutionSetupException {

    List<PlanFragment> fragments = Lists.newArrayList();

    PlanFragment rootFragment = null;
    FragmentRoot rootOperator = null;

    long queryStartTime = System.currentTimeMillis();
    int timeZone = DateUtility.getIndex(System.getProperty("user.timezone"));

    // now we generate all the individual plan fragments and associated assignments. Note, we need all endpoints
    // assigned before we can materialize, so we start a new loop here rather than utilizing the previous one.
    for (Wrapper wrapper : planningSet) {
      Fragment node = wrapper.getNode();
      Stats stats = node.getStats();
      final PhysicalOperator physicalOperatorRoot = node.getRoot();
      boolean isRootNode = rootNode == node;

      if (isRootNode && wrapper.getWidth() != 1)
        throw new FragmentSetupException(
            String.format(
                "Failure while trying to setup fragment.  The root fragment must always have parallelization one.  In the current case, the width was set to %d.",
                wrapper.getWidth()));
      // a fragment is self driven if it doesn't rely on any other exchanges.
      boolean isLeafFragment = node.getReceivingExchangePairs().size() == 0;

      // Create a minorFragment for each major fragment.
      for (int minorFragmentId = 0; minorFragmentId < wrapper.getWidth(); minorFragmentId++) {
        IndexedFragmentNode iNode = new IndexedFragmentNode(minorFragmentId, wrapper);
        wrapper.resetAllocation();
        PhysicalOperator op = physicalOperatorRoot.accept(materializer, iNode);
        Preconditions.checkArgument(op instanceof FragmentRoot);
        FragmentRoot root = (FragmentRoot) op;

        // get plan as JSON
        String plan;
        String optionsData;
        try {
          plan = reader.writeJson(root);
          optionsData = reader.writeJson(options);
        } catch (JsonProcessingException e) {
          throw new FragmentSetupException("Failure while trying to convert fragment into json.", e);
        }

        FragmentHandle handle = FragmentHandle //
            .newBuilder() //
            .setMajorFragmentId(wrapper.getMajorFragmentId()) //
            .setMinorFragmentId(minorFragmentId) //
            .setQueryId(queryId) //
            .build();
        PlanFragment fragment = PlanFragment.newBuilder() //
            .setCpuCost(stats.getCpuCost()) //
            .setDiskCost(stats.getDiskCost()) //
            .setForeman(foremanNode) //
            .setMemoryCost(stats.getMemoryCost()) //
            .setNetworkCost(stats.getNetworkCost()) //
            .setFragmentJson(plan) //
            .setHandle(handle) //
            .setAssignment(wrapper.getAssignedEndpoint(minorFragmentId)) //
            .setLeafFragment(isLeafFragment) //
            .setQueryStartTime(queryStartTime)
            .setTimeZone(timeZone)//
            .setMemInitial(wrapper.getInitialAllocation())//
            .setMemMax(wrapper.getMaxAllocation())
            .setOptionsJson(optionsData)
            .build();

        if (isRootNode) {
          logger.debug("Root fragment:\n {}", StringEscapeUtils.unescapeJava(fragment.toString()));
          rootFragment = fragment;
          rootOperator = root;
        } else {
          logger.debug("Remote fragment:\n {}", StringEscapeUtils.unescapeJava(fragment.toString()));
          fragments.add(fragment);
        }
      }
    }

    return new QueryWorkUnit(rootOperator, rootFragment, fragments);
  }

  private void assignEndpoints(Collection<DrillbitEndpoint> allNodes, PlanningSet planningSet) throws PhysicalOperatorSetupException {
    // First we determine the amount of parallelization for a fragment. This will be between 1 and maxWidth based on
    // cost. (Later could also be based on cluster operation.) then we decide endpoints based on affinity (later this
    // could be based on endpoint load)
    for (Wrapper wrapper : planningSet) {

      Stats stats = wrapper.getStats();

      // figure out width.
      int width = Math.min(stats.getMaxWidth(), globalMaxWidth);
      float diskCost = stats.getDiskCost();
//      logger.debug("Frag max width: {} and diskCost: {}", stats.getMaxWidth(), diskCost);

      // TODO: right now we'll just assume that each task is cost 1 so we'll set the breadth at the lesser of the number
      // of tasks or the maximum width of the fragment.
      if (diskCost < width) {
//        width = (int) diskCost;
      }

      width = Math.min(width, maxWidthPerEndpoint*allNodes.size());

      if (width < 1) width = 1;
//      logger.debug("Setting width {} on fragment {}", width, wrapper);
      wrapper.setWidth(width);
      // figure out endpoint assignments. also informs the exchanges about their respective endpoints.
      wrapper.assignEndpoints(allNodes, affinityFactor);
    }
  }

}
