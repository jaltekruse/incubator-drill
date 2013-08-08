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

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MockScanBatchCreator;

import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.metadata.ParquetMetadata;

public class ParquetScanBatchCreator implements BatchCreator<ParquetRowGroupScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockScanBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, ParquetRowGroupScan config, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    List<RecordReader> readers = Lists.newArrayList();
    ParquetMetadata readFooter = null;
    Path path;
    Configuration configuration;
    for(ParquetRowGroupScan.RowGroupReadEntry e : config.getRowGroupReadEntries()){
      /*
      TODO - to prevent reading the footer again in the parquet record reader (it is read earlier in the ParquetStorageEngine)
      we should add more information to the RowGroupInfo that will be populated upon the first read to
      provide the reader with all of th file meta-data it needs
      These fields will be added to the constructor below
      */
      // TODO - change the API on the record reader and update this
      // TODO - this also involves adding all of the fields necessary for reading the file to the RowGroupInfo and ParquetRowGroupReadEntry classes
      readers.add(new ParquetRecordReader(context, e.getPath(), e.getRowGroupIndex(), e.getDFSname()));
    }
    return new ScanBatch(context, readers.iterator());
  }
}
