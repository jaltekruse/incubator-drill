package org.apache.drill.exec.store.dfs;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;

/**
 * A Storage engine associated with a Hadoop FileSystem Implementation. Examples include HDFS, MapRFS, QuantacastFileSystem,
 * LocalFileSystem, as well Apache Drill specific CachedFileSystem, ClassPathFileSystem and LocalSyncableFileSystem.
 * Tables are file names, directories and path patterns. This storage engine delegates to FSFormatEngines but shares
 * references to the FileSystem configuration and path management.
 */
public class DFSStorageEngine {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DFSStorageEngine.class);

  private FileSystem fs;
  private Map<Pattern, FormatPlugin> fileMatchers;
  private Map<String, FormatPlugin> enginesByName;
  
  

}
