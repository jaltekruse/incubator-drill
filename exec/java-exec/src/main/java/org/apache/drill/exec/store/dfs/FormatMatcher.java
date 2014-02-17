package org.apache.drill.exec.store.dfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;

public abstract class FormatMatcher {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatMatcher.class);

  public abstract boolean isDirReadable(FileStatus dir) throws IOException;
  public abstract boolean isFileReadable(FileStatus file) throws IOException;
  public abstract FormatPlugin getFormatPlugin();
}
