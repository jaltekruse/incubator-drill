package org.apache.drill.exec.store.dfs.easy;


public interface FileWork {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileWork.class);
  
  public String getPath();  
  public long getStart();
  public long getLength();  
}
