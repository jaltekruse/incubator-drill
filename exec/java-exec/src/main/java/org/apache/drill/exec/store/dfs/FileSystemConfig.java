package org.apache.drill.exec.store.dfs;

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.logical.StorageEngineConfig;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class FileSystemConfig implements StorageEngineConfig{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemConfig.class);
  
  public String connection;
  public List<Workspace> workspaces;
  
  public static class Workspace{
    
    
    public Workspace(String name, String path) {
      super();
      this.name = name;
      this.path = path;
    }
    public String name;
    public String path; 
  }
  
  
  @JsonIgnore
  public List<Workspace> getWorkspaceList(){
    if(workspaces != null && !workspaces.isEmpty()) return workspaces;
    return DEFAULT;
  }
  
  private static final List<Workspace> DEFAULT = Collections.singletonList(new Workspace("default", "/"));
  
}
