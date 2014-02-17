package org.apache.drill.exec.store.dfs.shim.fallback;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.dfs.shim.DrillInputStream;
import org.apache.drill.exec.store.dfs.shim.DrillOutputStream;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

public class FallbackFileSystem extends DrillFileSystem {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FallbackFileSystem.class);

  final FileSystem fs;

  public FallbackFileSystem(DrillConfig config, FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public FileSystem getUnderlying() {
    return fs;
  }

  @Override
  public List<FileStatus> list(boolean recursive, Path... paths) throws IOException {
    if (recursive) {
      List<FileStatus> statuses = Lists.newArrayList();
      for (Path p : paths) {
        addRecursiveStatus(fs.getFileStatus(p), statuses);
      }
      return statuses;

    } else {
      return Lists.newArrayList(fs.listStatus(paths));
    }
  }

  private void addRecursiveStatus(FileStatus parent, List<FileStatus> listToFill) throws IOException {
    if (parent.isDir()) {
      for (FileStatus s : fs.listStatus(parent.getPath())) {
        addRecursiveStatus(s, listToFill);
      }
    } else {
      listToFill.add(parent);
    }
  }

  @Override
  public FileStatus getFileStatus(Path p) throws IOException {
    return fs.getFileStatus(p);
  }

  @Override
  public DrillOutputStream create(Path p) throws IOException {
    return new Out(fs.create(p));
  }

  @Override
  public DrillInputStream open(Path p) throws IOException {
    return new In(fs.open(p));
  }

  @Override
  public void close() throws Exception {
    fs.close();
  }

  @Override
  public BlockLocation[] getBlockLocations(FileStatus status, long start, long len) throws IOException {
    return fs.getFileBlockLocations(status, start, len);
  }

  private class Out extends DrillOutputStream {

    private final FSDataOutputStream out;
    
    public Out(FSDataOutputStream out) {
      super();
      this.out = out;
    }

    @Override
    public void close() throws Exception {
      out.close();
    }

    @Override
    public FSDataOutputStream getOuputStream() {
      return out;
    }

  }

  private class In extends DrillInputStream {

    private final FSDataInputStream in;
    
    public In(FSDataInputStream in) {
      super();
      this.in = in;
    }

    @Override
    public FSDataInputStream getInputStream() {
      return in;
    }

    @Override
    public void close() throws Exception {
      in.close();
    }

  }

}
