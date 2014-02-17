package org.apache.drill.exec.store.json;

import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.Size;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ScanEntry {
  
  static int ESTIMATED_RECORD_SIZE = 1024; // 1kb
  private final String path;
  private Size size;

  @JsonCreator
  public ScanEntry(@JsonProperty("path") String path) {
    this.path = path;
    size = new Size(ESTIMATED_RECORD_SIZE, ESTIMATED_RECORD_SIZE);
  }

  public OperatorCost getCost() {
    return new OperatorCost(1, 1, 2, 2);
  }

  @JsonIgnore
  public Size getSize() {
    return size;
  }

  @JsonIgnore
  public String getPath() {
    return path;
  }
}