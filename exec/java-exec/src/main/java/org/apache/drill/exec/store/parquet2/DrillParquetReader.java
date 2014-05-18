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
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import parquet.hadoop.*;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.*;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class DrillParquetReader implements RecordReader {

  private ParquetMetadata footer;
  private MessageType schema;
  private ParquetRecordReader<DrillParquetRecord> reader;
  private Configuration conf;
  private boolean hasRemainder = false;
  private List<ValueVector> vectors;
  private RowGroupReadEntry entry;
  private List<SchemaPath> columns;

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
      for (String[] path : projection.getPaths()) {
        Type type = projection.getType(path);
        PrimitiveType pType;
        if (type.isPrimitive()) {
          pType = (PrimitiveType) type;
          MajorType mType = toMajorType(pType);
          ValueVector v = output.addField(MaterializedField.create(SchemaPath.getCompoundPath(path), mType),
                  (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(mType.getMinorType(), mType.getMode()));
          vectors.add(v);
        }
      }
      ParquetInputSplit split = new ParquetInputSplit(
              new Path(entry.getPath()),
              entry.getStart(),
              entry.getLength(),
              null,
              footer.getBlocks(),
              projection.toString().intern(),
              footer.getFileMetaData().getSchema().toString().intern(),
              null,
              null
      );
      reader = new ParquetRecordReader(new DrillReadSupport(vectors));
      reader.initialize(split, conf, Reporter.NULL);
      this.vectors = vectors;
      output.setNewSchema();
    } catch (IOException | InterruptedException | SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    try {
      for (ValueVector v : vectors) {
        v.allocateNew();
      }
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
      while(reader.nextKeyValue() && count < 5000) {
        if (reader.getCurrentValue().success) {
          count++;
        } else {
          hasRemainder = true;
          break;
        }
      }
      for (ValueVector v : vectors) {
        v.getMutator().setValueCount(count);
      }
      return count;
    } catch (IOException | InterruptedException e) {
      throw new DrillRuntimeException(e);
    }
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
