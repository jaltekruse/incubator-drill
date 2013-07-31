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
package org.apache.drill.exec.physical.config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.ReadEntry;
import org.apache.drill.exec.physical.ReadEntryWithPath;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.AbstractStorageEngine;
import org.apache.drill.exec.store.RecordReader;

import com.google.common.collect.ListMultimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.page.PageReadStore;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;

public class ParquetStorageEngine extends AbstractStorageEngine{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorageEngine.class);

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public ParquetScan getPhysicalScan(Scan scan) throws IOException {
    ArrayList<ReadEntryWithPath> readEntries = scan.getSelection().getListWith(new ObjectMapper(),
        new TypeReference<ArrayList<ReadEntryWithPath>>() {});
    ArrayList<ParquetScan.ParquetReadEntry> pReadEnties = new ArrayList();
    long start = 0, end = 0;
    ColumnChunkMetaData columnChunkMetaData;
    for (ReadEntryWithPath readEntryWithPath : readEntries){

      File file = new File(readEntryWithPath.getPath()).getAbsoluteFile();

      Path path = new Path(file.toURI());
      Configuration configuration = new Configuration();

      ParquetMetadata footer = ParquetFileReader.readFooter(configuration, path);
      readEntryWithPath.getPath();

      for (BlockMetaData rowGroup : footer.getBlocks()){
        // need to grab block information from HDFS
        columnChunkMetaData = rowGroup.getColumns().iterator().next();
        start = Math.min(columnChunkMetaData.getDictionaryPageOffset(), columnChunkMetaData.getFirstDataPageOffset());

        pReadEnties.add(new ParquetScan.ParquetReadEntry(readEntryWithPath.getPath(), start, end));
      }

    }

    return new ParquetScan(pReadEnties);
  }

  @Override
  public ListMultimap<ReadEntry, DrillbitEndpoint> getReadLocations(Collection<ReadEntry> entries) {
    return null;
  }

  @Override
  public RecordReader getReader(FragmentContext context, ReadEntry readEntry) throws IOException {
    return null;
  }



}
