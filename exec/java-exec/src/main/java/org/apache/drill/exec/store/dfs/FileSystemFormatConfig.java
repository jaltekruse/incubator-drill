package org.apache.drill.exec.store.dfs;

import org.apache.drill.common.logical.StorageEngineConfig;

public class FileSystemFormatConfig<T extends FormatConfig> implements StorageEngineConfig{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemFormatConfig.class);
  
  public T getFormatConfig(){
    return null;
  }
}
