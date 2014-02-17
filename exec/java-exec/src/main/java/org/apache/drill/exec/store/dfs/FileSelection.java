package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.internal.Lists;

public abstract class FileSelection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSelection.class);
  
  public abstract List<FileStatus> getFileList() throws IOException;
  
  public boolean isDir(){
    return false;
  }
  
  public FileStatus getStatus(){
    throw new UnsupportedOperationException();
  }
    
  public static class SingleFileSelection extends FileSelection{
    private FileStatus file;
    
    public SingleFileSelection(FileStatus file){
      this.file = file;
    }
    
    public List<FileStatus> getFileList(){
      return Collections.singletonList(file);
    }
    
  }
  
  public static class DirectorySelection extends FileSelection{
    private FileStatus dir;
    private DrillFileSystem fs;
    
    public DirectorySelection(FileStatus dir, DrillFileSystem fs){
      this.fs = fs;
      this.dir = dir;
    }
    
    public List<FileStatus> getFileList() throws IOException{
      return fs.list(true, dir.getPath());
    }
    
    public boolean isDir(){
      return true;
    }

    public FileStatus getStatus(){
      return dir;
    }
    
  }
  
  public static class PatternSelection extends FileSelection{
    private final Path pattern;
    private final DrillFileSystem fs;
    
    public PatternSelection(Path pattern, DrillFileSystem fs) {
      super();
      this.pattern = pattern;
      this.fs = fs;
    }

    @Override
    public List<FileStatus> getFileList() throws IOException {
      return fs.listByPattern(true, pattern);
    }
    
    
  }
  
  public static FileSelection create(DrillFileSystem fs, Path parent, String path) throws IOException{
    if(Pattern.quote(path).equals(path)){
      Path p = new Path(parent, path);
      FileStatus status = fs.getFileStatus(p);
      if(status.isDir()){
        return new DirectorySelection(status, fs);
      }else{
        return new SingleFileSelection(status);
      }
    }else{
      return new PatternSelection(new Path(parent, path), fs);
    }
  }
  

  
}
