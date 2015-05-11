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
package org.apache.drill.exec.planner.physical;

import org.apache.calcite.plan.Context;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.TypeValidators.BooleanValidator;
import org.apache.drill.exec.server.options.TypeValidators.DoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.LongValidator;
import org.apache.drill.exec.server.options.TypeValidators.PositiveLongValidator;
import org.apache.drill.exec.server.options.TypeValidators.RangeDoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.RangeLongValidator;

public class PlannerSettings implements Context{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlannerSettings.class);

  private int numEndPoints = 0;
  private boolean useDefaultCosting = false; // True: use default Optiq costing, False: use Drill costing
  private boolean forceSingleMode;

  public static final int MAX_BROADCAST_THRESHOLD = Integer.MAX_VALUE;
  public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 1024;

  public static final BooleanValidator CONSTANT_FOLDING = new BooleanValidator("planner.enable_constant_folding", true);
  public static final BooleanValidator DISABLE_EXCHANGE = new BooleanValidator("planner.disable_exchanges", false);
  public static final BooleanValidator HASHAGG = new BooleanValidator("planner.enable_hashagg", true);
  public static final BooleanValidator STREAMAGG = new BooleanValidator("planner.enable_streamagg", true);
  public static final BooleanValidator HASHJOIN = new BooleanValidator("planner.enable_hashjoin", true);
  public static final BooleanValidator MERGEJOIN = new BooleanValidator("planner.enable_mergejoin", true);
  public static final BooleanValidator NESTEDLOOPJOIN = new BooleanValidator("planner.enable_nestedloopjoin", true);
  public static final BooleanValidator MULTIPHASE = new BooleanValidator("planner.enable_multiphase_agg", true);
  public static final BooleanValidator BROADCAST = new BooleanValidator("planner.enable_broadcast_join", true);
  public static final LongValidator BROADCAST_THRESHOLD = new PositiveLongValidator("planner.broadcast_threshold",
      MAX_BROADCAST_THRESHOLD, 10000000);
  public static final DoubleValidator BROADCAST_FACTOR = new RangeDoubleValidator("planner.broadcast_factor", 0,
      Double.MAX_VALUE, 1.0d);
  public static final DoubleValidator NESTEDLOOPJOIN_FACTOR = new RangeDoubleValidator("planner.nestedloopjoin_factor",
      0, Double.MAX_VALUE, 100.0d);
  public static final BooleanValidator NLJOIN_FOR_SCALAR = new BooleanValidator(
      "planner.enable_nljoin_for_scalar_only", true);
  public static final DoubleValidator JOIN_ROW_COUNT_ESTIMATE_FACTOR = new RangeDoubleValidator(
      "planner.join.row_count_estimate_factor", 0, Double.MAX_VALUE, 1.0d);
  public static final BooleanValidator MUX_EXCHANGE = new BooleanValidator("planner.enable_mux_exchange", true);
  public static final BooleanValidator DEMUX_EXCHANGE = new BooleanValidator("planner.enable_demux_exchange", false);
  public static final LongValidator PARTITION_SENDER_THREADS_FACTOR = new LongValidator(
      "planner.partitioner_sender_threads_factor", 2);
  public static final LongValidator PARTITION_SENDER_MAX_THREADS = new LongValidator(
      "planner.partitioner_sender_max_threads", 8);
  public static final LongValidator PARTITION_SENDER_SET_THREADS = new LongValidator(
      "planner.partitioner_sender_set_threads", -1);
  public static final BooleanValidator PRODUCER_CONSUMER = new BooleanValidator("planner.add_producer_consumer", false);
  public static final LongValidator PRODUCER_CONSUMER_QUEUE_SIZE = new LongValidator(
      "planner.producer_consumer_queue_size", 10);
  public static final BooleanValidator HASH_SINGLE_KEY = new BooleanValidator("planner.enable_hash_single_key", true);
  public static final BooleanValidator HASH_JOIN_SWAP = new BooleanValidator("planner.enable_hashjoin_swap", true);
  public static final DoubleValidator HASH_JOIN_SWAP_MARGIN_FACTOR = new RangeDoubleValidator(
      "planner.join.hash_join_swap_margin_factor", 0, 100, 10d);
  public static final BooleanValidator ENABLE_DECIMAL_DATA_TYPE = new BooleanValidator("planner.enable_decimal_data_type", false);
  public static final BooleanValidator HEP_JOIN_OPT = new BooleanValidator("planner.enable_hep_join_opt", true);

  public static final LongValidator IDENTIFIER_MAX_LENGTH =
      new RangeLongValidator("planner.identifier_max_length", 128 /* A minimum length is needed because option names are identifiers themselves */,
                              Integer.MAX_VALUE, DEFAULT_IDENTIFIER_MAX_LENGTH);

  public OptionManager options = null;
  public FunctionImplementationRegistry functionImplementationRegistry = null;

  public PlannerSettings(OptionManager options, FunctionImplementationRegistry functionImplementationRegistry){
    this.options = options;
    this.functionImplementationRegistry = functionImplementationRegistry;
  }

  public OptionManager getOptions() {
    return options;
  }

  public boolean isSingleMode() {
    return forceSingleMode || options.getOption(DISABLE_EXCHANGE);
  }

  public void forceSingleMode() {
    forceSingleMode = true;
  }

  public int numEndPoints() {
    return numEndPoints;
  }

  public double getRowCountEstimateFactor(){
    return options.getOption(JOIN_ROW_COUNT_ESTIMATE_FACTOR);
  }

  public double getBroadcastFactor(){
    return options.getOption(BROADCAST_FACTOR);
  }

  public double getNestedLoopJoinFactor(){
    return options.getOption(NESTEDLOOPJOIN_FACTOR);
  }

  public boolean isNlJoinForScalarOnly() {
    return options.getOption(NLJOIN_FOR_SCALAR);
  }

  public boolean useDefaultCosting() {
    return useDefaultCosting;
  }

  public void setNumEndPoints(int numEndPoints) {
    this.numEndPoints = numEndPoints;
  }

  public void setUseDefaultCosting(boolean defcost) {
    this.useDefaultCosting = defcost;
  }

  public boolean isHashAggEnabled() {
    return options.getOption(HASHAGG);
  }

  public boolean isConstantFoldingEnabled() {
    return options.getOption(CONSTANT_FOLDING);
  }

  public boolean isStreamAggEnabled() {
    return options.getOption(STREAMAGG);
  }

  public boolean isHashJoinEnabled() {
    return options.getOption(HASHJOIN);
  }

  public boolean isMergeJoinEnabled() {
    return options.getOption(MERGEJOIN);
  }

  public boolean isNestedLoopJoinEnabled() {
    return options.getOption(NESTEDLOOPJOIN);
  }

  public boolean isMultiPhaseAggEnabled() {
    return options.getOption(MULTIPHASE);
  }

  public boolean isBroadcastJoinEnabled() {
    return options.getOption(BROADCAST);
  }

  public boolean isHashSingleKey() {
    return options.getOption(HASH_SINGLE_KEY);
  }

  public boolean isHashJoinSwapEnabled() {
    return options.getOption(HASH_JOIN_SWAP);
  }

  public boolean isHepJoinOptEnabled() {
    return options.getOption(HEP_JOIN_OPT);
  }

  public double getHashJoinSwapMarginFactor() {
    return options.getOption(HASH_JOIN_SWAP_MARGIN_FACTOR) / 100d;
  }

  public long getBroadcastThreshold() {
    return options.getOption(BROADCAST_THRESHOLD);
  }

  public long getSliceTarget(){
    return options.getOption(ExecConstants.PLANNER_SLICE_TARGET);
  }

  public boolean isMemoryEstimationEnabled() {
    return options.getOption(ExecConstants.ENABLE_MEMORY_ESTIMATION);
  }

  public String getFsPartitionColumnLabel() {
    return options.getOption(ExecConstants.FILESYSTEM_PARTITION_COLUMN_LABEL);
  }

  public long getIdentifierMaxLength(){
    return options.getOption(IDENTIFIER_MAX_LENGTH);
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if(clazz == PlannerSettings.class){
      return (T) this;
    }else{
      return null;
    }
  }


}
