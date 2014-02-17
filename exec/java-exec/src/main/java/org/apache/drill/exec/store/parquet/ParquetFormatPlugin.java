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
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.BasicFormatMatcher;
import org.apache.drill.exec.store.dfs.FileSystemFormatConfig;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.MagicString;
import org.apache.drill.exec.store.dfs.ReadHandle;
import org.apache.drill.exec.store.mock.MockStorageEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.CodecFactoryExposer;
import parquet.hadoop.ParquetFileWriter;

import com.google.common.collect.Lists;

public class ParquetFormatPlugin implements FormatPlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockStorageEngine.class);

  private final DrillbitContext context;
  static final ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
  private CodecFactoryExposer codecFactoryExposer;
  private final FileSystem fs;
  private final ParquetFormatMatcher formatMatcher;
  private FileSystemFormatConfig<ParquetFormatConfig> config;
  
  public ParquetFormatPlugin(FileSystem fs, FileSystemFormatConfig<ParquetFormatConfig> configuration, DrillbitContext context){
    this.context = context;
    this.codecFactoryExposer = new CodecFactoryExposer(fs.getConf());
    this.config = configuration;
    this.formatMatcher = new ParquetFormatMatcher(fs);
    this.fs = fs;
  }

  Configuration getHadoopConfig() {
    return fs.getConf();
  }

  FileSystem getFileSystem() {
    return fs;
  }

  public FileSystemFormatConfig<ParquetFormatConfig> getFormatConfig() {
    return config;
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }
  
  @Override
  public ParquetGroupScan getGroupScan(FileSystem fs, FieldReference outputRef, List<FileStatus> data) throws IOException {
    return new ParquetGroupScan( data, this, outputRef);
  }

  public CodecFactoryExposer getCodecFactoryExposer() {
    return codecFactoryExposer;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public FormatMatcher getMatcher() {
    return null;
  }
  
  

  
  private static class ParquetFormatMatcher extends BasicFormatMatcher{

    public ParquetFormatMatcher(FileSystem fs) {
      super(fs, //
          Lists.newArrayList( //
              Pattern.compile(".*\\.parquet$"), //
              Pattern.compile(".*/" + ParquetFileWriter.PARQUET_METADATA_FILE) //
              //
              ),
          Lists.newArrayList(new MagicString(0, ParquetFileWriter.MAGIC))
                    
          );
      
    }

    @Override
    public ReadHandle isDirReadable(FileStatus dir) {
      
      return super.isDirReadable(dir);
    }
    
    
    
  }
  
}