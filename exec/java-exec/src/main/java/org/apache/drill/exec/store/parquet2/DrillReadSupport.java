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

import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

import java.util.List;
import java.util.Map;

public class DrillReadSupport extends ReadSupport<DrillParquetRecord> {

  List<ValueVector> vectors;

  public DrillReadSupport() {

  }

  public DrillReadSupport(List<ValueVector> vectors) {
    this.vectors = vectors;
  }

  @Override
  public RecordMaterializer<DrillParquetRecord> prepareForRead(Configuration entries, Map<String, String> stringStringMap, MessageType messageType, ReadContext readContext) {
    return new DrillParquetRecordMaterializer(vectors, messageType);
  }

  private static String REQUESTED_PROJECTION = "parquet.projection";

  @Override
  public ReadContext init(InitContext context) {
    String requestedProjectionString = context.getConfiguration().get(REQUESTED_PROJECTION);

    if (requestedProjectionString != null && !requestedProjectionString.trim().isEmpty()) {
      MessageType requestedProjection = getSchemaForRead(context.getFileSchema(), requestedProjectionString);
      return new ReadContext(requestedProjection);
    } else {
      MessageType fileSchema = context.getFileSchema();
      return new ReadContext(fileSchema);
    }
  }
}
