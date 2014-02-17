package org.apache.drill.exec.store.parquet;

import org.apache.drill.exec.store.dfs.ReadEntryFromHDFS;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RowGroupReadEntry extends ReadEntryFromHDFS {

  private int rowGroupIndex;

  @parquet.org.codehaus.jackson.annotate.JsonCreator
  public RowGroupReadEntry(@JsonProperty("path") String path, @JsonProperty("start") long start,
                           @JsonProperty("length") long length, @JsonProperty("rowGroupIndex") int rowGroupIndex) {
    super(path, start, length);
    this.rowGroupIndex = rowGroupIndex;
  }

  @JsonIgnore
  public RowGroupReadEntry getRowGroupReadEntry() {
    return new RowGroupReadEntry(this.getPath(), this.getStart(), this.getLength(), this.rowGroupIndex);
  }

  public int getRowGroupIndex(){
    return rowGroupIndex;
  }
}