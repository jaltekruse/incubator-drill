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
package org.apache.drill.exec.physical.impl.orderedpartitioner;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.cache.Counter;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.cache.DistributedMultiMap;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.OrderedPartitionSender;
import org.apache.drill.exec.physical.impl.sort.SortBatch;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.sort.Sorter;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.eigenbase.rel.RelFieldCollation.Direction;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

/**
 * The purpose of this operator is to generate an ordered partition, rather than a random hash partition. This could be
 * used to do a total order sort, for example. This operator reads in a few incoming record batches, samples these
 * batches, and stores them in the distributed cache. The samples from all the parallel-running fragments are merged,
 * and a partition-table is built and stored in the distributed cache for use by all fragments. A new column is added to
 * the outgoing batch, whose value is determined by where each record falls in the partition table. This column is used
 * by PartitionSenderRootExec to determine which bucket to assign each record to.
 */
public class OrderedPartitionRecordBatch extends AbstractRecordBatch<OrderedPartitionSender> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderedPartitionRecordBatch.class);

  public final MappingSet mainMapping = new MappingSet((String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP,
      ClassGenerator.DEFAULT_SCALAR_MAP);
  public final MappingSet incomingMapping = new MappingSet("inIndex", null, "incoming", null,
      ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  public final MappingSet partitionMapping = new MappingSet("partitionIndex", null, "partitionVectors", null,
      ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);

  private static long MAX_SORT_BYTES = 8l * 1024 * 1024 * 1024;
  private final int recordsToSample; // How many records must be received before analyzing
  private final int samplingFactor; // Will collect samplingFactor * number of partitions to send to distributed cache
  private final float completionFactor; // What fraction of fragments must be completed before attempting to build
                                        // partition table
  protected final RecordBatch incoming;
  private boolean first = true;
  private OrderedPartitionProjector projector;
  private VectorContainer partitionVectors = new VectorContainer();
  private int partitions;
  private Queue<VectorContainer> batchQueue;
  private int recordsSampled;
  private int sendingMajorFragmentWidth;
  private boolean startedUnsampledBatches = false;
  private boolean upstreamNone = false;
  private int recordCount;

  private final IntVector partitionKeyVector;
  private final DistributedMap<VectorAccessibleSerializable> tableMap;
  private final Counter minorFragmentSampleCount;
  private final DistributedMultiMap<VectorAccessibleSerializable> mmap;
  private final String mapKey;
  private List<VectorContainer> sampledIncomingBatches;

  public OrderedPartitionRecordBatch(OrderedPartitionSender pop, RecordBatch incoming, FragmentContext context) {
    super(pop, context);
    this.incoming = incoming;
    this.partitions = pop.getDestinations().size();
    this.sendingMajorFragmentWidth = pop.getSendingWidth();
    this.recordsToSample = pop.getRecordsToSample();
    this.samplingFactor = pop.getSamplingFactor();
    this.completionFactor = pop.getCompletionFactor();

    DistributedCache cache = context.getDrillbitContext().getCache();
    this.mmap = cache.getMultiMap(VectorAccessibleSerializable.class);
    this.tableMap = cache.getMap(VectorAccessibleSerializable.class);
    Preconditions.checkNotNull(tableMap);

    this.mapKey = String.format("%s_%d", context.getHandle().getQueryId(), context.getHandle().getMajorFragmentId());
    this.minorFragmentSampleCount = cache.getCounter(mapKey);
    
    SchemaPath outputPath = new SchemaPath(popConfig.getRef().getPath(), ExpressionPosition.UNKNOWN);
    MaterializedField outputField = MaterializedField.create(outputPath, Types.required(TypeProtos.MinorType.INT));
    this.partitionKeyVector = (IntVector) TypeHelper.getNewVector(outputField, context.getAllocator());
    
  }

  
  @Override
  public void cleanup() {
    super.cleanup();
    this.partitionVectors.clear();
    this.partitionKeyVector.clear();
  }


  private boolean saveSamples() throws SchemaChangeException, ClassTransformationException, IOException {
    recordsSampled = 0;
    IterOutcome upstream;

    // Start collecting batches until recordsToSample records have been collected
    
    SortRecordBatchBuilder builder = new SortRecordBatchBuilder(context.getAllocator(), MAX_SORT_BYTES);
    builder.add(incoming);
    
    recordsSampled += incoming.getRecordCount();

    outer: while (recordsSampled < recordsToSample) {
      upstream = incoming.next();
      switch (upstream) {
      case NONE:
      case NOT_YET:
      case STOP:
        upstreamNone = true;
        break outer;
      default:
        // fall through
      }
      builder.add(incoming);
      recordsSampled += incoming.getRecordCount();
      if (upstream == IterOutcome.NONE)
        break;
    }
  VectorContainer sortedSamples = new VectorContainer();
    builder.build(context, sortedSamples);

    // Sort the records according the orderings given in the configuration

    Sorter sorter = SortBatch.createNewSorter(context, popConfig.getOrderings(), sortedSamples);
    SelectionVector4 sv4 = builder.getSv4();
    sorter.setup(context, sv4, sortedSamples);
    sorter.sort(sv4, sortedSamples);

    // Project every Nth record to a new vector container, where N = recordsSampled/(samplingFactor * partitions).
    // Uses the
    // the expressions from the Orderings to populate each column. There is one column for each Ordering in
    // popConfig.orderings.

    VectorContainer containerToCache = new VectorContainer();
    SampleCopier copier = getCopier(sv4, sortedSamples, containerToCache, popConfig.getOrderings());
    copier.copyRecords(recordsSampled / (samplingFactor * partitions), 0, samplingFactor * partitions);

    for (VectorWrapper<?> vw : containerToCache) {
      vw.getValueVector().getMutator().setValueCount(copier.getOutputRecords());
    }
    containerToCache.setRecordCount(copier.getOutputRecords());

    // Get a distributed multimap handle from the distributed cache, and put the vectors from the new vector container
    // into a serializable wrapper object, and then add to distributed map

    WritableBatch batch = WritableBatch.getBatchNoHVWrap(containerToCache.getRecordCount(), containerToCache, false);
    VectorAccessibleSerializable sampleToSave = new VectorAccessibleSerializable(batch, context.getAllocator());

    mmap.put(mapKey, sampleToSave);
    this.sampledIncomingBatches = builder.getHeldRecordBatches();
    builder.clear();
    batch.clear();
    containerToCache.clear();
    sampleToSave.clear();
    return true;
    
    
  }

  /**
   * This method is called when the first batch comes in. Incoming batches are collected until a threshold is met. At
   * that point, the records in the batches are sorted and sampled, and the sampled records are stored in the
   * distributed cache. Once a sufficient fraction of the fragments have shared their samples, each fragment grabs all
   * the samples, sorts all the records, builds a partition table, and attempts to push the partition table to the
   * distributed cache. Whichever table gets pushed first becomes the table used by all fragments for partitioning.
   * 
   * @return True is successful. False if failed.
   */
  private boolean getPartitionVectors() {


    try {

      if (!saveSamples()){
        return false;
      }
      
      VectorAccessibleSerializable finalTable = null;

      long val = minorFragmentSampleCount.incrementAndGet();
      logger.debug("Incremented mfsc, got {}", val);

      final long fragmentsBeforeProceed = (long) Math.ceil(sendingMajorFragmentWidth * completionFactor);
      final String finalTableKey = mapKey + "final";

      if (val == fragmentsBeforeProceed) { // we crossed the barrier, build table and get data.
        buildTable();
        finalTable = tableMap.get(finalTableKey);
      } else {
        // Wait until sufficient number of fragments have submitted samples, or proceed after xx ms passed
        // TODO: this should be polling.

        if (val < fragmentsBeforeProceed)
          Thread.sleep(10);
        for (int i = 0; i < 100 && finalTable == null; i++) {
          finalTable = tableMap.get(finalTableKey);
          if (finalTable != null){
            break;
          }
          Thread.sleep(10);
        }
        if (finalTable == null){
          buildTable();
        }
        finalTable = tableMap.get(finalTableKey);
      }

      Preconditions.checkState(finalTable != null);

      // Extract vectors from the wrapper, and add to partition vectors. These vectors will be used for partitioning in
      // the rest of this operator
      for (VectorWrapper<?> w : finalTable.get()) {
        partitionVectors.add(w.getValueVector());
      }
    } catch (ClassTransformationException | IOException | SchemaChangeException | InterruptedException ex) {
      kill();
      logger.error("Failure while building final partition table.", ex);
      context.fail(ex);
      return false;
    }
    return true;
  }

  private void buildTable() throws SchemaChangeException, ClassTransformationException, IOException {

    // Get all samples from distributed map
    
    SortRecordBatchBuilder containerBuilder = new SortRecordBatchBuilder(context.getAllocator(), MAX_SORT_BYTES);
    for (VectorAccessibleSerializable w : mmap.get(mapKey)) {
      containerBuilder.add(w.get());
    }
    VectorContainer allSamplesContainer = new VectorContainer();
    containerBuilder.build(context, allSamplesContainer);

    List<Ordering> orderDefs = Lists.newArrayList();
    int i = 0;
    for (Ordering od : popConfig.getOrderings()) {
      SchemaPath sp = new SchemaPath("f" + i++, ExpressionPosition.UNKNOWN);
      orderDefs.add(new Ordering(od.getDirection(), new FieldReference(sp)));
    }

    // sort the data incoming samples.
    SelectionVector4 newSv4 = containerBuilder.getSv4();
    Sorter sorter = SortBatch.createNewSorter(context, orderDefs, allSamplesContainer);
    sorter.setup(context, newSv4, allSamplesContainer);
    sorter.sort(newSv4, allSamplesContainer);

    // Copy every Nth record from the samples into a candidate partition table, where N = totalSampledRecords/partitions
    // Attempt to push this to the distributed map. Only the first candidate to get pushed will be used.
    VectorContainer candidatePartitionTable = new VectorContainer();
    SampleCopier copier = getCopier(newSv4, allSamplesContainer, candidatePartitionTable, orderDefs);
    int skipRecords = containerBuilder.getSv4().getTotalCount() / partitions;
    copier.copyRecords(skipRecords, skipRecords, partitions - 1);
    assert copier.getOutputRecords() == partitions - 1 : String.format("output records: %d partitions: %d", copier.getOutputRecords(), partitions);
    for (VectorWrapper<?> vw : candidatePartitionTable) {
      vw.getValueVector().getMutator().setValueCount(copier.getOutputRecords());
    }
    candidatePartitionTable.setRecordCount(copier.getOutputRecords());
    WritableBatch batch = WritableBatch.getBatchNoHVWrap(candidatePartitionTable.getRecordCount(), candidatePartitionTable, false);
    VectorAccessibleSerializable wrap = new VectorAccessibleSerializable(batch, context.getDrillbitContext().getAllocator());
    tableMap.putIfAbsent(mapKey + "final", wrap, 1, TimeUnit.MINUTES);
    
    candidatePartitionTable.clear();
    allSamplesContainer.clear();
    containerBuilder.clear();
    wrap.clear();

  }

  /**
   * Creates a copier that does a project for every Nth record from a VectorContainer incoming into VectorContainer
   * outgoing. Each Ordering in orderings generates a column, and evaluation of the expression associated with each
   * Ordering determines the value of each column. These records will later be sorted based on the values in each
   * column, in the same order as the orderings.
   * 
   * @param sv4
   * @param incoming
   * @param outgoing
   * @param orderings
   * @return
   * @throws SchemaChangeException
   */
  private SampleCopier getCopier(SelectionVector4 sv4, VectorContainer incoming, VectorContainer outgoing,
      List<Ordering> orderings) throws SchemaChangeException {
    List<ValueVector> localAllocationVectors = Lists.newArrayList();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final ClassGenerator<SampleCopier> cg = CodeGenerator.getRoot(SampleCopier.TEMPLATE_DEFINITION,
        context.getFunctionRegistry());

    int i = 0;
    for (Ordering od : orderings) {
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), incoming, collector, context.getFunctionRegistry());
      SchemaPath schemaPath = new SchemaPath("f" + i++, ExpressionPosition.UNKNOWN);
      TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder().mergeFrom(expr.getMajorType())
          .clearMode().setMode(TypeProtos.DataMode.REQUIRED);
      TypeProtos.MajorType newType = builder.build();
      MaterializedField outputField = MaterializedField.create(schemaPath, newType);
      if (collector.hasErrors()) {
        throw new SchemaChangeException(String.format(
            "Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }

      ValueVector vector = TypeHelper.getNewVector(outputField, context.getAllocator());
      localAllocationVectors.add(vector);
      TypedFieldId fid = outgoing.add(vector);
      ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr);
      cg.addExpr(write);
      logger.debug("Added eval.");
    }
    for (ValueVector vv : localAllocationVectors) {
      AllocationHelper.allocate(vv, samplingFactor * partitions, 50);
    }
    outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    try {
      SampleCopier sampleCopier = context.getImplementationClass(cg);
      sampleCopier.setupCopier(context, sv4, incoming, outgoing);
      return sampleCopier;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException(e);
    }
  }

  @Override
  protected void killIncoming() {
    incoming.kill();
  }

  @Override
  public IterOutcome next() {
    container.zeroVectors();

    // if we got IterOutcome.NONE while getting partition vectors, and there are no batches on the queue, then we are
    // done
    if (upstreamNone && (batchQueue == null || batchQueue.size() == 0))
      return IterOutcome.NONE;

    // if there are batches on the queue, process them first, rather than calling incoming.next()
    if (batchQueue != null && batchQueue.size() > 0) {
      VectorContainer vc = batchQueue.poll();
      recordCount = vc.getRecordCount();
      try {

        // Must set up a new schema each time, because ValueVectors are not reused between containers in queue
        setupNewSchema(vc);
      } catch (SchemaChangeException ex) {
        kill();
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      }
      doWork(vc);
      vc.zeroVectors();
      return IterOutcome.OK_NEW_SCHEMA;
    }

    // Reaching this point, either this is the first iteration, or there are no batches left on the queue and there are
    // more incoming
    IterOutcome upstream = incoming.next();

    if (this.first && upstream == IterOutcome.OK) {
      throw new RuntimeException("Invalid state: First batch should have OK_NEW_SCHEMA");
    }

    // If this is the first iteration, we need to generate the partition vectors before we can proceed
    if (this.first && upstream == IterOutcome.OK_NEW_SCHEMA) {
      if (!getPartitionVectors()){
        cleanup();
        return IterOutcome.STOP;   
      }
      
      batchQueue = new LinkedBlockingQueue<>(this.sampledIncomingBatches);
      first = false;

      // Now that we have the partition vectors, we immediately process the first batch on the queue
      VectorContainer vc = batchQueue.poll();
      try {
        setupNewSchema(vc);
      } catch (SchemaChangeException ex) {
        kill();
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      }
      doWork(vc);
      vc.zeroVectors();
      recordCount = vc.getRecordCount();
      return IterOutcome.OK_NEW_SCHEMA;
    }

    // if this now that all the batches on the queue are processed, we begin processing the incoming batches. For the
    // first one
    // we need to generate a new schema, even if the outcome is IterOutcome.OK After that we can reuse the schema.
    if (this.startedUnsampledBatches == false) {
      this.startedUnsampledBatches = true;
      if (upstream == IterOutcome.OK)
        upstream = IterOutcome.OK_NEW_SCHEMA;
    }
    switch (upstream) {
    case NONE:
    case NOT_YET:
    case STOP:
      cleanup();
      recordCount = 0;
      return upstream;
    case OK_NEW_SCHEMA:
      try {
        setupNewSchema(incoming);
      } catch (SchemaChangeException ex) {
        kill();
        logger.error("Failure during query", ex);
        context.fail(ex);
        return IterOutcome.STOP;
      }
      // fall through.
    case OK:
      doWork(incoming);
      recordCount = incoming.getRecordCount();
      return upstream; // change if upstream changed, otherwise normal.
    default:
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  protected void doWork(VectorAccessible batch) {
    int recordCount = batch.getRecordCount();
    AllocationHelper.allocate(partitionKeyVector, recordCount, 50);
    projector.projectRecords(recordCount, 0);
    for (VectorWrapper<?> v : container) {
      ValueVector.Mutator m = v.getValueVector().getMutator();
      m.setValueCount(recordCount);
    }
  }

  /**
   * Sets up projection that will transfer all of the columns in batch, and also populate the partition column based on
   * which partition a record falls into in the partition table
   * 
   * @param batch
   * @throws SchemaChangeException
   */
  protected void setupNewSchema(VectorAccessible batch) throws SchemaChangeException {
    container.clear();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();

    final ClassGenerator<OrderedPartitionProjector> cg = CodeGenerator.getRoot(
        OrderedPartitionProjector.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    for (VectorWrapper<?> vw : batch) {
      TransferPair tp = vw.getValueVector().getTransferPair();
      transfers.add(tp);
      container.add(tp.getTo());
    }

    cg.setMappingSet(mainMapping);

    int count = 0;
    for (Ordering od : popConfig.getOrderings()) {
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, context.getFunctionRegistry());
      if (collector.hasErrors())
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      cg.setMappingSet(incomingMapping);
      ClassGenerator.HoldingContainer left = cg.addExpr(expr, false);
      cg.setMappingSet(partitionMapping);
      ClassGenerator.HoldingContainer right = cg.addExpr(
          new ValueVectorReadExpression(new TypedFieldId(expr.getMajorType(), count++)), false);
      cg.setMappingSet(mainMapping);

      FunctionCall f = new FunctionCall(ComparatorFunctions.COMPARE_TO, ImmutableList.of(
          (LogicalExpression) new HoldingContainerExpression(left), new HoldingContainerExpression(right)),
          ExpressionPosition.UNKNOWN);
      ClassGenerator.HoldingContainer out = cg.addExpr(f, false);
      JConditional jc = cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.Ascending) {
        jc._then()._return(out.getValue());
      } else {
        jc._then()._return(out.getValue().minus());
      }
    }

    cg.getEvalBlock()._return(JExpr.lit(0));

    container.add(this.partitionKeyVector);
    container.buildSchema(batch.getSchema().getSelectionVectorMode());

    try {
      this.projector = context.getImplementationClass(cg);
      projector.setup(context, batch, this, transfers, partitionVectors, partitions, new SchemaPath(popConfig.getRef().getPath(), ExpressionPosition.UNKNOWN));
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }

}
