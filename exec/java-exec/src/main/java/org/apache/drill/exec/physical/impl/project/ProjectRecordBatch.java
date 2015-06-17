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
package org.apache.drill.exec.physical.impl.project;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.CastFunctions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.google.common.collect.Maps;

public class ProjectRecordBatch extends AbstractSingleRecordBatch<Project> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectRecordBatch.class);

  private Projector projector;
  private List<ValueVector> allocationVectors = Lists.newArrayList();
  private List<ComplexWriter> complexWriters;
  private boolean hasRemainder = false;
  private int remainderIndex = 0;
  private int recordCount;

  public static final String EMPTY_STRING = "";
  private boolean first = true;
  private NewSchemaHandler newSchemaHandler = new NewSchemaHandler();

  private class NewSchemaHandler extends ProjectorNewSchemaHandler<Projector, ProjectRecordBatch> {
    @Override
    public List<NamedExpression> getExpressionList() {
      if (popConfig.getExprs() != null) {
        return popConfig.getExprs();
      }

      final List<NamedExpression> exprs = Lists.newArrayList();
      for (final MaterializedField field : incoming.getSchema()) {
        if (Types.isComplex(field.getType()) || Types.isRepeated(field.getType())) {
          final LogicalExpression convertToJson = FunctionCallFactory.createConvert(ConvertExpression.CONVERT_TO, "JSON", field.getPath(), ExpressionPosition.UNKNOWN);
          final String castFuncName = CastFunctions.getCastFunc(TypeProtos.MinorType.VARCHAR);
          final List<LogicalExpression> castArgs = Lists.newArrayList();
          castArgs.add(convertToJson);  //input_expr
        /*
         * We are implicitly casting to VARCHAR so we don't have a max length,
         * using an arbitrary value. We trim down the size of the stored bytes
         * to the actual size so this size doesn't really matter.
         */
          castArgs.add(new ValueExpressions.LongExpression(TypeHelper.VARCHAR_DEFAULT_CAST_LEN, null)); //
          final FunctionCall castCall = new FunctionCall(castFuncName, castArgs, ExpressionPosition.UNKNOWN);
          exprs.add(new NamedExpression(castCall, new FieldReference(field.getPath())));
        } else {
          exprs.add(new NamedExpression(field.getPath(), new FieldReference(field.getPath())));
        }
      }
      return exprs;
    }


    @Override
    void generateCodeAndSetup(ClassGenerator<Projector> cg, FragmentContext context,
        RecordBatch incoming, ProjectRecordBatch recordBatch, List<TransferPair> transfers)
            throws ClassTransformationException, IOException, SchemaChangeException {
      projector = context.getImplementationClass(cg.getCodeGenerator());
      projector.setup(context, incoming, recordBatch, transfers);
    }
  }

  static class ClassifierResult {
    public boolean isStar = false;
    public List<String> outputNames;
    public String prefix = "";
    public HashMap<String, Integer> prefixMap = Maps.newHashMap();
    public CaseInsensitiveMap outputMap = new CaseInsensitiveMap();
    final CaseInsensitiveMap sequenceMap = new CaseInsensitiveMap();

    void clear() {
      isStar = false;
      prefix = "";
      if (outputNames != null) {
        outputNames.clear();
      }

      // note:  don't clear the internal maps since they have cumulative data..
    }
  }

  public ProjectRecordBatch(final Project pop, final RecordBatch incoming, final FragmentContext context) throws OutOfMemoryException {
    super(pop, context, incoming);
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }


  @Override
  protected void killIncoming(final boolean sendUpstream) {
    super.killIncoming(sendUpstream);
    hasRemainder = false;
  }


  @Override
  public IterOutcome innerNext() {
    if (hasRemainder) {
      handleRemainder();
      return IterOutcome.OK;
    }
    return super.innerNext();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    return this.container;
  }

  @Override
  protected IterOutcome doWork() {
    int incomingRecordCount = incoming.getRecordCount();

    if (first && incomingRecordCount == 0) {
      if (complexWriters != null) {
        IterOutcome next = null;
        while (incomingRecordCount == 0) {
          next = next(incoming);
          if (next == IterOutcome.OUT_OF_MEMORY) {
            outOfMemory = true;
            return next;
          } else if (next != IterOutcome.OK && next != IterOutcome.OK_NEW_SCHEMA) {
            return next;
          }
          incomingRecordCount = incoming.getRecordCount();
        }
        if (next == IterOutcome.OK_NEW_SCHEMA) {
          try {
            setupNewSchema();
          } catch (final SchemaChangeException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    first = false;

    container.zeroVectors();

    if (!doAlloc()) {
      outOfMemory = true;
      return IterOutcome.OUT_OF_MEMORY;
    }

    final int outputRecords = projector.projectRecords(0, incomingRecordCount, 0);
    if (outputRecords < incomingRecordCount) {
      setValueCount(outputRecords);
      hasRemainder = true;
      remainderIndex = outputRecords;
      this.recordCount = remainderIndex;
    } else {
      setValueCount(incomingRecordCount);
      for(final VectorWrapper<?> v: incoming) {
        v.clear();
      }
      this.recordCount = outputRecords;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }

    return IterOutcome.OK;
  }

  private void handleRemainder() {
    final int remainingRecordCount = incoming.getRecordCount() - remainderIndex;
    if (!doAlloc()) {
      outOfMemory = true;
      return;
    }
    final int projRecords = projector.projectRecords(remainderIndex, remainingRecordCount, 0);
    if (projRecords < remainingRecordCount) {
      setValueCount(projRecords);
      this.recordCount = projRecords;
      remainderIndex += projRecords;
    } else {
      setValueCount(remainingRecordCount);
      hasRemainder = false;
      remainderIndex = 0;
      for (final VectorWrapper<?> v : incoming) {
        v.clear();
      }
      this.recordCount = remainingRecordCount;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }
  }

  public void addComplexWriter(final ComplexWriter writer) {
    complexWriters.add(writer);
  }

  private boolean doAlloc() {
    //Allocate vv in the allocationVectors.
    for (final ValueVector v : this.allocationVectors) {
      AllocationHelper.allocateNew(v, incoming.getRecordCount());
    }

    //Allocate vv for complexWriters.
    if (complexWriters == null) {
      return true;
    }

    for (final ComplexWriter writer : complexWriters) {
      writer.allocate();
    }

    return true;
  }

  private void setValueCount(final int count) {
    for (final ValueVector v : allocationVectors) {
      final ValueVector.Mutator m = v.getMutator();
      m.setValueCount(count);
    }

    if (complexWriters == null) {
      return;
    }

    for (final ComplexWriter writer : complexWriters) {
      writer.setValueCount(count);
    }
  }


  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    final FunctionImplementationRegistry functionImplementationRegistry = context.getFunctionRegistry();
    final ClassGenerator<Projector> cg =
        CodeGenerator.getRoot(Projector.TEMPLATE_DEFINITION, functionImplementationRegistry);
    return newSchemaHandler.setupNewSchema(allocationVectors, complexWriters, container,
        functionImplementationRegistry, incoming, callBack, context, cg, this);
  }

}
