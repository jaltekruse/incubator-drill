package org.apache.drill.exec.store.dfs;

import org.apache.hadoop.fs.FileStatus;

public class FileReadHandle extends ReadHandle{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileReadHandle.class);
  
  private FileStatus status;

  public FileReadHandle(FileStatus status) {
    super();
    this.status = status;
  }

  public FileStatus getStatus() {
    return status;
  }
  
  
}
