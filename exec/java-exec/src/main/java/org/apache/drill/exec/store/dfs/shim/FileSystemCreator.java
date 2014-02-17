package org.apache.drill.exec.store.dfs.shim;

import java.io.IOException;
import java.net.URI;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.store.dfs.shim.fallback.FallbackFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class FileSystemCreator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemCreator.class);
  
  public static DrillFileSystem getFileSystem(DrillConfig config, URI connectionString, Configuration fsConf) throws IOException{
    FileSystem fs = FileSystem.get(connectionString, fsConf);
    return new FallbackFileSystem(config, fs);
  }
  
}
