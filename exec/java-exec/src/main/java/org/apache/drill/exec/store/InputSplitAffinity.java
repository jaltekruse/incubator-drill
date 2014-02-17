package org.apache.drill.exec.store;

import org.apache.hadoop.mapreduce.InputSplit;

public class InputSplitAffinity<T extends InputSplit> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InputSplitAffinity.class);

  private final T split;

  public InputSplitAffinity(T split) {
    super();
    this.split = split;
  }
  
  
}
