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
package org.apache.drill.exec.store.parquet2;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.impl.filter.Filterer;
import org.apache.drill.exec.physical.impl.filter.ReturnValueExpression;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.parquet.ParquetRowGroupScan;
import org.apache.drill.exec.store.parquet.ParquetTypeHelper;
import org.apache.drill.exec.store.parquet.ParquetTypeHelper.BigIntConverter;
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import org.apache.drill.exec.store.parquet2.DrillParquetRecordConverter.DrillPrimitiveConverter;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.impl.ColumnReaderImpl;
import parquet.hadoop.*;
import parquet.hadoop.ColumnChunkIncReadStore.*;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.*;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class DrillParquetReader implements RecordReader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillParquetReader.class);

  private ParquetMetadata footer;
  private MessageType schema;
  private Configuration conf;
  private RowGroupReadEntry entry;
  private List<SchemaPath> columns;
  private ParquetRowGroupScan popConfig;
  private FragmentContext context;

  private List<ColumnReader> columnReaders = Lists.newArrayList();
  private List<ColumnReader> filterColumnReaders = Lists.newArrayList();
  private List<ColumnReader> nonFilterColumnReaders = Lists.newArrayList();
  private List<DrillPrimitiveConverter> converters = Lists.newArrayList();
  private List<ColumnDescriptor> columnDescriptors = Lists.newArrayList();

  private VectorContainer container = new VectorContainer();
  private ParquetFilter filterer;
  private List<TypedFieldId> filterIds;

  private long totalCount;

  public DrillParquetReader(ParquetMetadata footer, RowGroupReadEntry entry, List<SchemaPath> columns, Configuration conf, ParquetRowGroupScan popConfig, FragmentContext context) {
    this.footer = footer;
    this.conf = conf;
    this.columns = columns;
    this.entry = entry;
    this.popConfig = popConfig;
    this.context = context;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      schema = footer.getFileMetaData().getSchema();
      List<ValueVector> vectors = Lists.newArrayList();
      MessageType projection;

      if (columns == null || columns.size() == 0) {
        projection = schema;
      } else {
        List<Type> types = Lists.newArrayList();
        for (SchemaPath path : columns) {
          for (String[] p : schema.getPaths()) {
            //TODO extend this to handle nested types
            if (p[0].equalsIgnoreCase(path.getAsUnescapedPath())) {
              types.add(schema.getType(p));
              break;
            }
          }
        }
        projection = new MessageType("projection", types);
      }

      Map<ColumnPath, ColumnChunkMetaData> paths = new HashMap();

      for (ColumnChunkMetaData md : footer.getBlocks().get(entry.getRowGroupIndex()).getColumns()) {
        paths.put(md.getPath(), md);
      }

      CodecFactoryExposer codecFactoryExposer = new CodecFactoryExposer(conf);
      FileSystem fs = FileSystem.get(conf);
      Path filePath = new Path(entry.getPath());

      for (String[] path : projection.getPaths()) {
        Type type = projection.getType(path);
        PrimitiveType pType;
        if (type.isPrimitive()) {
          pType = (PrimitiveType) type;
          MajorType mType = toMajorType(pType);
          ValueVector v = output.addField(MaterializedField.create(SchemaPath.getCompoundPath(path), mType),
                  (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(mType.getMinorType(), mType.getMode()));
          container.add(v);
        }
      }
      container.buildSchema(SelectionVectorMode.NONE);
      this.filterer = generateFilter();


      for (String[] path : projection.getPaths()) {
        TypedFieldId id = container.getValueVectorId(SchemaPath.getCompoundPath(path));
        ValueVector v = container.getValueAccessorById(TypeHelper.getValueVectorClass(id.getFinalType().getMinorType(), id.getFinalType().getMode()), id.getFieldIds()).getValueVector();
        DrillPrimitiveConverter converter = ParquetTypeHelper.getConverterForVector(v);
        converters.add(converter);
        FSDataInputStream in = fs.open(filePath);
        ColumnChunkMetaData md = paths.get(ColumnPath.get(path));
        in.seek(md.getStartingPos());

        ColumnChunkIncPageReader pageReader = new ColumnChunkIncPageReader(md, in, codecFactoryExposer.getCodecFactory());
        ColumnDescriptor columnDescriptor = projection.getColumnDescription(path);
        columnDescriptors.add(columnDescriptor);
        ColumnReader reader = new ColumnReaderImpl(columnDescriptor, pageReader, converter);
        columnReaders.add(reader);
        if (filterIds.contains(id)) {
          filterColumnReaders.add(reader);
        } else {
          nonFilterColumnReaders.add(reader);
        }
      }

      filterer.doSetup(context, container);

      this.totalCount = footer.getBlocks().get(entry.getRowGroupIndex()).getRowCount();

    } catch (IOException | SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  protected ParquetFilter generateFilter() throws SchemaChangeException {
    final ErrorCollector collector = new ErrorCollectorImpl();
    final ClassGenerator<ParquetFilter> cg = CodeGenerator.getRoot(ParquetFilter.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    final LogicalExpression expr = ExpressionTreeMaterializer.materialize(popConfig.getFilter(), container, collector, context.getFunctionRegistry());
    if(collector.hasErrors()){
      throw new SchemaChangeException(String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
    }

    cg.addExpr(new ReturnValueExpression(expr));

    try {
      ParquetFilter filterer = context.getImplementationClass(cg);
      filterer.doSetup(context, container);
      filterIds = cg.evaluationVisitor.ids;
      return filterer;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }

  }

  private long totalRead = 0;
  @Override
  public int next() {
    Stopwatch watch = new Stopwatch();
    watch.start();
    int count = 0;
    for (DrillPrimitiveConverter converter : converters) {
      converter.index = 0;
    }
    outer: for (; count < 32*1024 && totalRead < totalCount; totalRead++) {
      for (ColumnReader reader : filterColumnReaders) {
        reader.writeCurrentValueToConverter();
      }
      if (filterer.doEval(count, 0)) {
        for (ColumnReader reader : nonFilterColumnReaders) {
          if (reader.getCurrentDefinitionLevel() == reader.getDescriptor().getMaxDefinitionLevel()) {
            reader.writeCurrentValueToConverter();
          }
        }
        for (ColumnReader reader : columnReaders) {
          reader.consume();
        }
        for (DrillPrimitiveConverter converter : converters) {
          converter.index++;
        }
        count++;
      } else {
        for (ColumnReader reader : filterColumnReaders) {
          reader.consume();
        }
        for (ColumnReader reader : nonFilterColumnReaders) {
          reader.skip();
        }
      }
      for (DrillPrimitiveConverter converter : converters) {
        converter.reset();
      }
    }

    for (VectorWrapper v : container) {
      v.getValueVector().getMutator().setValueCount(count);
    }
    long t = watch.elapsed(TimeUnit.NANOSECONDS);
    if (count > 0) {
      logger.debug("Took {} ns to read {} records. {} ns / record", t, count, t / count);
    }
    return count;
  }

  static TypeProtos.MajorType toMajorType(PrimitiveType pType) {
    OriginalType originalType = pType.getOriginalType();
    PrimitiveTypeName primitiveTypeName = pType.getPrimitiveTypeName();
    DataMode mode = null;
    int length = pType.getTypeLength();
    switch (pType.getRepetition()) {

      case OPTIONAL:
        switch (pType.getPrimitiveTypeName()) {
          case BINARY:
            if (originalType == null) {
              return Types.optional(TypeProtos.MinorType.VARBINARY);
            }
            switch (originalType) {
              case UTF8:
                return Types.optional(MinorType.VARCHAR);
              case DECIMAL:
                return Types.withScaleAndPrecision(getDecimalType(pType), DataMode.OPTIONAL, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, originalType));
            }
          case INT64:
            if (originalType == null) {
              return Types.optional(TypeProtos.MinorType.BIGINT);
            }
            switch(originalType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL18, DataMode.OPTIONAL, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
              case FINETIME:
                throw new UnsupportedOperationException();
              case TIMESTAMP:
                return Types.optional(MinorType.TIMESTAMP);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, originalType));
            }
          case INT32:
            if (originalType == null) {
              return Types.optional(TypeProtos.MinorType.INT);
            }
            switch(originalType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL9, DataMode.OPTIONAL, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
              case DATE:
                return Types.optional(MinorType.DATE);
              case TIME:
                return Types.optional(MinorType.TIME);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, originalType));
            }
          case BOOLEAN:
            return Types.optional(TypeProtos.MinorType.BIT);
          case FLOAT:
            return Types.optional(TypeProtos.MinorType.FLOAT4);
          case DOUBLE:
            return Types.optional(TypeProtos.MinorType.FLOAT8);
          // TODO - Both of these are not supported by the parquet library yet (7/3/13),
          // but they are declared here for when they are implemented
          case INT96:
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY).setWidth(12)
                    .setMode(mode).build();
          case FIXED_LEN_BYTE_ARRAY:
            if (originalType == null) {
              return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY)
                      .setWidth(length).setMode(mode).build();
            } else if (originalType == OriginalType.DECIMAL) {
              return Types.withScaleAndPrecision(getDecimalType(pType), DataMode.OPTIONAL, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
            }
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
      case REQUIRED:
        switch (primitiveTypeName) {
          case BINARY:
            if (originalType == null) {
              return Types.required(TypeProtos.MinorType.VARBINARY);
            }
            switch (originalType) {
              case UTF8:
                return Types.required(MinorType.VARCHAR);
              case DECIMAL:
                return Types.withScaleAndPrecision(getDecimalType(pType), DataMode.REQUIRED, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, originalType));
            }
          case INT64:
            if (originalType == null) {
              return Types.required(MinorType.BIGINT);
            }
            switch(originalType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL18, DataMode.REQUIRED, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
              case FINETIME:
                throw new UnsupportedOperationException();
              case TIMESTAMP:
                return Types.required(MinorType.TIMESTAMP);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, originalType));
            }
          case INT32:
            if (originalType == null) {
              return Types.required(MinorType.INT);
            }
            switch(originalType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL9, DataMode.REQUIRED, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
              case DATE:
                return Types.required(MinorType.DATE);
              case TIME:
                return Types.required(MinorType.TIME);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, originalType));
            }
          case BOOLEAN:
            return Types.required(TypeProtos.MinorType.BIT);
          case FLOAT:
            return Types.required(TypeProtos.MinorType.FLOAT4);
          case DOUBLE:
            return Types.required(TypeProtos.MinorType.FLOAT8);
          // Both of these are not supported by the parquet library yet (7/3/13),
          // but they are declared here for when they are implemented
          case INT96:
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY).setWidth(12)
                    .setMode(mode).build();
          case FIXED_LEN_BYTE_ARRAY:
            if (originalType == null) {
              checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
              return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY)
                      .setWidth(length).setMode(mode).build();
            } else if (originalType == OriginalType.DECIMAL) {
              return Types.withScaleAndPrecision(getDecimalType(pType), DataMode.REQUIRED, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
            }
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
      case REPEATED:
        switch (primitiveTypeName) {
          case BINARY:
            if (originalType == null) {
              return Types.repeated(TypeProtos.MinorType.VARBINARY);
            }
            switch (originalType) {
              case UTF8:
                return Types.repeated(MinorType.VARCHAR);
              case DECIMAL:
                return Types.withScaleAndPrecision(getDecimalType(pType), DataMode.REPEATED, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, originalType));
            }
          case INT64:
            if (originalType == null) {
              return Types.repeated(MinorType.BIGINT);
            }
            switch(originalType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL18, DataMode.REPEATED, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
              case FINETIME:
                throw new UnsupportedOperationException();
              case TIMESTAMP:
                return Types.repeated(MinorType.TIMESTAMP);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, originalType));
            }
          case INT32:
            if (originalType == null) {
              return Types.repeated(MinorType.INT);
            }
            switch(originalType) {
              case DECIMAL:
                return Types.withScaleAndPrecision(MinorType.DECIMAL9, DataMode.REPEATED, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
              case DATE:
                return Types.repeated(MinorType.DATE);
              case TIME:
                return Types.repeated(MinorType.TIME);
              default:
                throw new UnsupportedOperationException(String.format("unsupported type: %s %s", primitiveTypeName, originalType));
            }
          case BOOLEAN:
            return Types.repeated(TypeProtos.MinorType.BIT);
          case FLOAT:
            return Types.repeated(TypeProtos.MinorType.FLOAT4);
          case DOUBLE:
            return Types.repeated(TypeProtos.MinorType.FLOAT8);
          // Both of these are not supported by the parquet library yet (7/3/13),
          // but they are declared here for when they are implemented
          case INT96:
            return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY).setWidth(12)
                    .setMode(mode).build();
          case FIXED_LEN_BYTE_ARRAY:
            if (originalType == null) {
              checkArgument(length > 0, "A length greater than zero must be provided for a FixedBinary type.");
              return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FIXEDBINARY)
                      .setWidth(length).setMode(mode).build();
            } else if (originalType == OriginalType.DECIMAL) {
              return Types.withScaleAndPrecision(getDecimalType(pType), DataMode.REPEATED, pType.getDecimalMetadata().getScale(), pType.getDecimalMetadata().getPrecision());
            }
          default:
            throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName);
        }
    }
    throw new UnsupportedOperationException("Type not supported: " + primitiveTypeName + " Mode: " + mode);
  }

  private static MinorType getDecimalType(PrimitiveType pType) {
    return pType.getDecimalMetadata().getPrecision() <= 28 ? MinorType.DECIMAL28SPARSE : MinorType.DECIMAL38SPARSE;
  }

  @Override
  public void cleanup() {
  }
}
