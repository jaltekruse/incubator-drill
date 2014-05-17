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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.ValueVector;
import parquet.column.ColumnDescriptor;
import parquet.format.ConvertedType;
import parquet.format.SchemaElement;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.schema.PrimitiveType;

public class ParquetFixedWidthDictionaryReader extends ColumnReader{

  ParquetFixedWidthDictionaryReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
                                    ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, ValueVector v,
                                    SchemaElement schemaElement) throws ExecutionSetupException {
    super(parentReader, allocateSize, descriptor, columnChunkMetaData, fixedLength, v, schemaElement);
  }

  @Override
  public void readField(long recordsToReadInThisPass, ColumnReader firstColumnStatus) {

    recordsReadInThisIteration = Math.min(pageReadStatus.currentPage.getValueCount()
        - pageReadStatus.valuesRead, recordsToReadInThisPass - valuesReadInCurrentPass);
    int defLevel;
    for (int i = 0; i < recordsReadInThisIteration; i++){
      defLevel = pageReadStatus.definitionLevels.readInteger();
      // if the value is defined
      if (defLevel == columnDescriptor.getMaxDefinitionLevel()){
        if (columnDescriptor.getType() == PrimitiveType.PrimitiveTypeName.INT64)
          ((BigIntVector)valueVec).getMutator().set(i + valuesReadInCurrentPass,
              pageReadStatus.valueReader.readLong() );
      }
      // otherwise the value is skipped, because the bit vector indicating nullability is zero filled
    }
  }
}
