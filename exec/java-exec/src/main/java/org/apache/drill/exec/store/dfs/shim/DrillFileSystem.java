package org.apache.drill.exec.store.dfs.shim;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Wraps the underlying filesystem to provide advanced file system features. Delegates to underlying file system if
 * those features are exposed.
 */
public abstract class DrillFileSystem implements AutoCloseable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFileSystem.class);

  public abstract FileSystem getUnderlying();
  
  public abstract BlockLocation[] getBlockLocations(FileStatus status, long start, long length) throws IOException;
  public abstract List<FileStatus> list(boolean recursive, Path... paths) throws IOException;
  public abstract List<FileStatus> listByPattern(boolean recursive, Path pattern) throws IOException;
  public abstract FileStatus getFileStatus(Path p) throws IOException;
  public abstract DrillOutputStream create(Path p) throws IOException;
  public abstract DrillInputStream open(Path p) throws IOException;
  
}
