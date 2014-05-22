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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
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
  private List<ValueVector> vectors;
  private RowGroupReadEntry entry;
  private List<SchemaPath> columns;

  private List<ColumnReader> columnReaders = Lists.newArrayList();
  private List<DrillPrimitiveConverter> converters = Lists.newArrayList();

  private long totalCount;

  public DrillParquetReader(ParquetMetadata footer, RowGroupReadEntry entry, List<SchemaPath> columns, Configuration conf) {
    this.footer = footer;
    this.conf = conf;
    this.columns = columns;
    this.entry = entry;
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
          vectors.add(v);
          DrillPrimitiveConverter converter = ParquetTypeHelper.getConverterForVector(v);
          converters.add(converter);
          FSDataInputStream in = fs.open(filePath);
          ColumnChunkMetaData md = paths.get(ColumnPath.get(path));
          in.seek(md.getStartingPos());

          ColumnChunkIncPageReader pageReader = new ColumnChunkIncPageReader(md, in, codecFactoryExposer.getCodecFactory());
          ColumnReader reader = new ColumnReaderImpl(projection.getColumnDescription(path), pageReader, converter);
          columnReaders.add(reader);
        }
      }
//      ParquetInputSplit split = new ParquetInputSplit(
//              new Path(entry.getPath()),
//              entry.getStart(),
//              entry.getLength(),
//              null,
//              footer.getBlocks(),
//              projection.toString().intern(),
//              footer.getFileMetaData().getSchema().toString().intern(),
//              null,
//              null
//      );
//      reader = new ParquetRecordReader(new DrillReadSupport(vectors));
//      reader.initialize(split, conf, Reporter.NULL);

      this.vectors = vectors;

      this.totalCount = footer.getBlocks().get(entry.getRowGroupIndex()).getRowCount();

    } catch (IOException | SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  /*
  @Override
  public int next() {
    try {
      int count = 0;
      if (reader.getCurrentValue() != null) {
        reader.getCurrentValue().index = 0;
      }
      if (hasRemainder) {
        reader.getCurrentValue().write();
        if (!reader.getCurrentValue().success) {
          throw new DrillRuntimeException("Failed to read next record");
        }
        hasRemainder = false;
        count++;
      }
      Stopwatch watch = new Stopwatch();
      watch.start();
      while(reader.nextKeyValue() && count < 32*1024) {
        if (reader.getCurrentValue().success) {
          count++;
        } else {
          hasRemainder = true;
          break;
        }
      }
      long t = watch.elapsed(TimeUnit.NANOSECONDS);
      if (count > 0)
        logger.debug("Took {} ns to read {} records. {} ns / record", t, count, t / count);
      for (ValueVector v : vectors) {
        v.getMutator().setValueCount(count);
      }
      return count;
    } catch (IOException | InterruptedException e) {
      throw new DrillRuntimeException(e);
    }
  }
  */

  private long totalRead = 0;
  @Override
  public int next() {
    Stopwatch watch = new Stopwatch();
    watch.start();
    int count = 0;
    int width = columnReaders.size();
    outer: for (; count < 32*1024 && totalRead < totalCount; count++, totalRead++) {
      for (int i = 0; i < width; i++) {
        if (columnReaders.get(i).getCurrentDefinitionLevel() != 0){
          columnReaders.get(i).writeCurrentValueToConverter();
          if (!converters.get(i).write(count)) {
            break outer;
          }
        }
      }
      for (DrillPrimitiveConverter converter : converters) {
        converter.reset();
      }
      for (ColumnReader reader : columnReaders) {
        reader.consume();
      }
    }
    for (ValueVector v : vectors) {
      v.getMutator().setValueCount(count);
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
