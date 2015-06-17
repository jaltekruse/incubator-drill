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
package org.apache.drill.exec.physical.impl.project;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.fn.CastFunctions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.DrillComplexWriterFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.Project;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ProjectorNewSchemaHandler {

  public interface ExpressionListHolder {
    List<NamedExpression> getExpressionList();
  }

  private boolean isWildcard(final NamedExpression ex) {
    if ( !(ex.getExpr() instanceof SchemaPath)) {
      return false;
    }
    final NameSegment expr = ((SchemaPath)ex.getExpr()).getRootSegment();
    return expr.getPath().contains(StarColumnHelper.STAR_COLUMN);
  }

  private boolean isAnyWildcard(final List<NamedExpression> exprs) {
    for (final NamedExpression e : exprs) {
      if (isWildcard(e)) {
        return true;
      }
    }
    return false;
  }

  /** hack to make ref and full work together... need to figure out if this is still necessary. **/
  private FieldReference getRef(final NamedExpression e) {
    return e.getRef();
  }

  private boolean isClassificationNeeded(final List<NamedExpression> exprs) {
    boolean needed = false;
    for (int i = 0; i < exprs.size(); i++) {
      final NamedExpression ex = exprs.get(i);
      if (!(ex.getExpr() instanceof SchemaPath)) {
        continue;
      }
      final NameSegment expr = ((SchemaPath) ex.getExpr()).getRootSegment();
      final NameSegment ref = ex.getRef().getRootSegment();
      final boolean refHasPrefix = ref.getPath().contains(StarColumnHelper.PREFIX_DELIMITER);
      final boolean exprContainsStar = expr.getPath().contains(StarColumnHelper.STAR_COLUMN);

      if (refHasPrefix || exprContainsStar) {
        needed = true;
        break;
      }
    }
    return needed;
  }

  private String getUniqueName(final String name, final ProjectRecordBatch.ClassifierResult result) {
    final Integer currentSeq = (Integer) result.sequenceMap.get(name);
    if (currentSeq == null) { // name is unique, so return the original name
      final Integer n = -1;
      result.sequenceMap.put(name, n);
      return name;
    }
    // create a new name
    final Integer newSeq = currentSeq + 1;
    final String newName = name + newSeq;
    result.sequenceMap.put(name, newSeq);
    result.sequenceMap.put(newName, -1);

    return newName;
  }

  /**
   * Helper method to ensure unique output column names. If allowDupsWithRename is set to true, the original name
   * will be appended with a suffix number to ensure uniqueness. Otherwise, the original column would not be renamed even
   * even if it has been used
   *
   * @param origName            the original input name of the column
   * @param result              the data structure to keep track of the used names and decide what output name should be
   *                            to ensure uniqueness
   * @Param allowDupsWithRename if the original name has been used, is renaming allowed to ensure output name unique
   */
  private void addToResultMaps(final String origName, final ProjectRecordBatch.ClassifierResult result, final boolean allowDupsWithRename) {
    String name = origName;
    if (allowDupsWithRename) {
      name = getUniqueName(origName, result);
    }
    if (!result.outputMap.containsKey(name)) {
      result.outputNames.add(name);
      result.outputMap.put(name,  name);
    } else {
      result.outputNames.add(ProjectRecordBatch.EMPTY_STRING);
    }
  }

  // TODO - this will now have an expectation that the passed allocationVectors list is non-null

  public boolean setupNewSchema(
      List<ValueVector> allocationVectors,
      List<ComplexWriter> complexWriters,
      VectorContainer container,
      ExpressionListHolder expressionListHolder,
      FunctionImplementationRegistry functionImplementationRegistry,
      RecordBatch incoming,
      SchemaChangeCallBack callBack,


  ) throws SchemaChangeException {
    if (allocationVectors != null) {
      for (final ValueVector v : allocationVectors) {
        v.clear();
      }
    }
    allocationVectors.clear();
    if (complexWriters != null) {
      container.clear();
    } else {
      container.zeroVectors();
    }
    final List<NamedExpression> exprs = expressionListHolder.getExpressionList();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();

    final ClassGenerator<Projector> cg = CodeGenerator.getRoot(Projector.TEMPLATE_DEFINITION, functionImplementationRegistry);

    final IntOpenHashSet transferFieldIds = new IntOpenHashSet();

    final boolean isAnyWildcard = isAnyWildcard(exprs);

    final ProjectRecordBatch.ClassifierResult result = new ProjectRecordBatch.ClassifierResult();
    final boolean classify = isClassificationNeeded(exprs);

    for (int i = 0; i < exprs.size(); i++) {
      final NamedExpression namedExpression = exprs.get(i);
      result.clear();

      if (classify && namedExpression.getExpr() instanceof SchemaPath) {
        classifyExpr(namedExpression, incoming, result);

        if (result.isStar) {
          // The value indicates which wildcard we are processing now
          final Integer value = result.prefixMap.get(result.prefix);
          if (value != null && value.intValue() == 1) {
            int k = 0;
            for (final VectorWrapper<?> wrapper : incoming) {
              final ValueVector vvIn = wrapper.getValueVector();
              if (k > result.outputNames.size()-1) {
                assert false;
              }
              final String name = result.outputNames.get(k++);  // get the renamed column names
              if (name == ProjectRecordBatch.EMPTY_STRING) {
                continue;
              }
              final FieldReference ref = new FieldReference(name);
              final ValueVector vvOut = container.addOrGet(MaterializedField.create(ref, vvIn.getField().getType()));
              final TransferPair tp = vvIn.makeTransferPair(vvOut);
              transfers.add(tp);
            }
          } else if (value != null && value.intValue() > 1) { // subsequent wildcards should do a copy of incoming valuevectors
            int k = 0;
            for (final VectorWrapper<?> wrapper : incoming) {
              final ValueVector vvIn = wrapper.getValueVector();
              final SchemaPath originalPath = vvIn.getField().getPath();
              if (k > result.outputNames.size()-1) {
                assert false;
              }
              final String name = result.outputNames.get(k++);  // get the renamed column names
              if (name == ProjectRecordBatch.EMPTY_STRING) {
                continue;
              }

              final LogicalExpression expr = ExpressionTreeMaterializer.materialize(originalPath, incoming, collector, functionImplementationRegistry );
              if (collector.hasErrors()) {
                throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
              }

              final MaterializedField outputField = MaterializedField.create(name, expr.getMajorType());
              final ValueVector vv = container.addOrGet(outputField, callBack);
              allocationVectors.add(vv);
              final TypedFieldId fid = container.getValueVectorId(outputField.getPath());
              final ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
              final HoldingContainer hc = cg.addExpr(write);
            }
          }
          continue;
        }
      } else {
        // For the columns which do not needed to be classified,
        // it is still necessary to ensure the output column name is unique
        result.outputNames = Lists.newArrayList();
        final String outputName = getRef(namedExpression).getRootSegment().getPath();
        addToResultMaps(outputName, result, true);
      }

      String outputName = getRef(namedExpression).getRootSegment().getPath();
      if (result != null && result.outputNames != null && result.outputNames.size() > 0) {
        boolean isMatched = false;
        for (int j = 0; j < result.outputNames.size(); j++) {
          if (!result.outputNames.get(j).equals(ProjectRecordBatch.EMPTY_STRING)) {
            outputName = result.outputNames.get(j);
            isMatched = true;
            break;
          }
        }

        if(!isMatched) {
          continue;
        }
      }

      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(namedExpression.getExpr(), incoming, collector, functionImplementationRegistry, true);
      final MaterializedField outputField = MaterializedField.create(outputName, expr.getMajorType());
      if (collector.hasErrors()) {
        throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }

      // add value vector to transfer if direct reference and this is allowed, otherwise, add to evaluation stack.
      if (expr instanceof ValueVectorReadExpression && incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.NONE
          && !((ValueVectorReadExpression) expr).hasReadPath()
          && !isAnyWildcard
          && !transferFieldIds.contains(((ValueVectorReadExpression) expr).getFieldId().getFieldIds()[0])) {

        final ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
        final TypedFieldId id = vectorRead.getFieldId();
        final ValueVector vvIn = incoming.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();
        Preconditions.checkNotNull(incoming);

        final FieldReference ref = getRef(namedExpression);
        final ValueVector vvOut = container.addOrGet(MaterializedField.create(ref, vectorRead.getMajorType()));
        final TransferPair tp = vvIn.makeTransferPair(vvOut);
        transfers.add(tp);
        transferFieldIds.add(vectorRead.getFieldId().getFieldIds()[0]);
      } else if (expr instanceof DrillFuncHolderExpr &&
          ((DrillFuncHolderExpr) expr).isComplexWriterFuncHolder())  {
        // Need to process ComplexWriter function evaluation.
        // Lazy initialization of the list of complex writers, if not done yet.
        if (complexWriters == null) {
          complexWriters = Lists.newArrayList();
        } else {
          complexWriters.clear();
        }

        // The reference name will be passed to ComplexWriter, used as the name of the output vector from the writer.
        ((DrillComplexWriterFuncHolder) ((DrillFuncHolderExpr) expr).getHolder()).setReference(namedExpression.getRef());
        cg.addExpr(expr);
      } else {
        // need to do evaluation.
        final ValueVector vector = container.addOrGet(outputField, callBack);
        allocationVectors.add(vector);
        final TypedFieldId fid = container.getValueVectorId(outputField.getPath());
        final boolean useSetSafe = !(vector instanceof FixedWidthVector);
        final ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, useSetSafe);
        final HoldingContainer hc = cg.addExpr(write);

        // We cannot do multiple transfers from the same vector. However we still need to instantiate the output vector.
        if (expr instanceof ValueVectorReadExpression) {
          final ValueVectorReadExpression vectorRead = (ValueVectorReadExpression) expr;
          if (!vectorRead.hasReadPath()) {
            final TypedFieldId id = vectorRead.getFieldId();
            final ValueVector vvIn = incoming.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();
            vvIn.makeTransferPair(vector);
          }
        }
        logger.debug("Added eval for project expression.");
      }
    }

    try {
      this.projector = context.getImplementationClass(cg.getCodeGenerator());
      projector.setup(context, incoming, this, transfers);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
    if (container.isSchemaChanged()) {
      container.buildSchema(SelectionVectorMode.NONE);
      return true;
    } else {
      return false;
    }
  }

  private void classifyExpr(final NamedExpression ex, final RecordBatch incoming, final ProjectRecordBatch.ClassifierResult result)  {
    final NameSegment expr = ((SchemaPath)ex.getExpr()).getRootSegment();
    final NameSegment ref = ex.getRef().getRootSegment();
    final boolean exprHasPrefix = expr.getPath().contains(StarColumnHelper.PREFIX_DELIMITER);
    final boolean refHasPrefix = ref.getPath().contains(StarColumnHelper.PREFIX_DELIMITER);
    final boolean exprIsStar = expr.getPath().equals(StarColumnHelper.STAR_COLUMN);
    final boolean refContainsStar = ref.getPath().contains(StarColumnHelper.STAR_COLUMN);
    final boolean exprContainsStar = expr.getPath().contains(StarColumnHelper.STAR_COLUMN);
    final boolean refEndsWithStar = ref.getPath().endsWith(StarColumnHelper.STAR_COLUMN);

    String exprPrefix = ProjectRecordBatch.EMPTY_STRING;
    String exprSuffix = expr.getPath();

    if (exprHasPrefix) {
      // get the prefix of the expr
      final String[] exprComponents = expr.getPath().split(StarColumnHelper.PREFIX_DELIMITER, 2);
      assert(exprComponents.length == 2);
      exprPrefix = exprComponents[0];
      exprSuffix = exprComponents[1];
      result.prefix = exprPrefix;
    }

    boolean exprIsFirstWildcard = false;
    if (exprContainsStar) {
      result.isStar = true;
      final Integer value = (Integer) result.prefixMap.get(exprPrefix);
      if (value == null) {
        final Integer n = 1;
        result.prefixMap.put(exprPrefix, n);
        exprIsFirstWildcard = true;
      } else {
        final Integer n = value + 1;
        result.prefixMap.put(exprPrefix, n);
      }
    }

    final int incomingSchemaSize = incoming.getSchema().getFieldCount();

    // for debugging..
    // if (incomingSchemaSize > 9) {
    // assert false;
    // }

    // input is '*' and output is 'prefix_*'
    if (exprIsStar && refHasPrefix && refEndsWithStar) {
      final String[] components = ref.getPath().split(StarColumnHelper.PREFIX_DELIMITER, 2);
      assert(components.length == 2);
      final String prefix = components[0];
      result.outputNames = Lists.newArrayList();
      for(final VectorWrapper<?> wrapper : incoming) {
        final ValueVector vvIn = wrapper.getValueVector();
        final String name = vvIn.getField().getPath().getRootSegment().getPath();

        // add the prefix to the incoming column name
        final String newName = prefix + StarColumnHelper.PREFIX_DELIMITER + name;
        addToResultMaps(newName, result, false);
      }
    }
    // input and output are the same
    else if (expr.getPath().equals(ref.getPath()) && (!exprContainsStar || exprIsFirstWildcard)) {
      if (exprContainsStar && exprHasPrefix) {
        assert exprPrefix != null;

        int k = 0;
        result.outputNames = Lists.newArrayListWithCapacity(incomingSchemaSize);
        for (int j=0; j < incomingSchemaSize; j++) {
          result.outputNames.add(ProjectRecordBatch.EMPTY_STRING);  // initialize
        }

        for (final VectorWrapper<?> wrapper : incoming) {
          final ValueVector vvIn = wrapper.getValueVector();
          final String incomingName = vvIn.getField().getPath().getRootSegment().getPath();
          // get the prefix of the name
          final String[] nameComponents = incomingName.split(StarColumnHelper.PREFIX_DELIMITER, 2);
          // if incoming valuevector does not have a prefix, ignore it since this expression is not referencing it
          if (nameComponents.length <= 1) {
            k++;
            continue;
          }
          final String namePrefix = nameComponents[0];
          if (exprPrefix.equals(namePrefix)) {
            final String newName = incomingName;
            if (!result.outputMap.containsKey(newName)) {
              result.outputNames.set(k, newName);
              result.outputMap.put(newName,  newName);
            }
          }
          k++;
        }
      } else {
        result.outputNames = Lists.newArrayList();
        if (exprContainsStar) {
          for (final VectorWrapper<?> wrapper : incoming) {
            final ValueVector vvIn = wrapper.getValueVector();
            final String incomingName = vvIn.getField().getPath().getRootSegment().getPath();
            if (refContainsStar) {
              addToResultMaps(incomingName, result, true); // allow dups since this is likely top-level project
            } else {
              addToResultMaps(incomingName, result, false);
            }
          }
        } else {
          final String newName = expr.getPath();
          if (!refHasPrefix && !exprHasPrefix) {
            addToResultMaps(newName, result, true); // allow dups since this is likely top-level project
          } else {
            addToResultMaps(newName, result, false);
          }
        }
      }
    }

    // input is wildcard and it is not the first wildcard
    else if(exprIsStar) {
      result.outputNames = Lists.newArrayList();
      for (final VectorWrapper<?> wrapper : incoming) {
        final ValueVector vvIn = wrapper.getValueVector();
        final String incomingName = vvIn.getField().getPath().getRootSegment().getPath();
        addToResultMaps(incomingName, result, true); // allow dups since this is likely top-level project
      }
    }

    // only the output has prefix
    else if (!exprHasPrefix && refHasPrefix) {
      result.outputNames = Lists.newArrayList();
      final String newName = ref.getPath();
      addToResultMaps(newName, result, false);
    }
    // input has prefix but output does not
    else if (exprHasPrefix && !refHasPrefix) {
      int k = 0;
      result.outputNames = Lists.newArrayListWithCapacity(incomingSchemaSize);
      for (int j=0; j < incomingSchemaSize; j++) {
        result.outputNames.add(ProjectRecordBatch.EMPTY_STRING);  // initialize
      }

      for (final VectorWrapper<?> wrapper : incoming) {
        final ValueVector vvIn = wrapper.getValueVector();
        final String name = vvIn.getField().getPath().getRootSegment().getPath();
        final String[] components = name.split(StarColumnHelper.PREFIX_DELIMITER, 2);
        if (components.length <= 1)  {
          k++;
          continue;
        }
        final String namePrefix = components[0];
        final String nameSuffix = components[1];
        if (exprPrefix.equals(namePrefix)) {
          if (refContainsStar) {
            // remove the prefix from the incoming column names
            final String newName = getUniqueName(nameSuffix, result);  // for top level we need to make names unique
            result.outputNames.set(k, newName);
          } else if (exprSuffix.equals(nameSuffix)) {
            // example: ref: $f1, expr: T0<PREFIX><column_name>
            final String newName = ref.getPath();
            result.outputNames.set(k, newName);
          }
        } else {
          result.outputNames.add(ProjectRecordBatch.EMPTY_STRING);
        }
        k++;
      }
    }
    // input and output have prefixes although they could be different...
    else if (exprHasPrefix && refHasPrefix) {
      final String[] input = expr.getPath().split(StarColumnHelper.PREFIX_DELIMITER, 2);
      assert(input.length == 2);
      assert false : "Unexpected project expression or reference";  // not handled yet
    }
    else {
      // if the incoming schema's column name matches the expression name of the Project,
      // then we just want to pick the ref name as the output column name

      result.outputNames = Lists.newArrayList();
      for (final VectorWrapper<?> wrapper : incoming) {
        final ValueVector vvIn = wrapper.getValueVector();
        final String incomingName = vvIn.getField().getPath().getRootSegment().getPath();
        if (expr.getPath().equals(incomingName)) {
          final String newName = ref.getPath();
          addToResultMaps(newName, result, true);
        }
      }
    }
  }
}
