package org.apache.drill.exec.store.dfs.hadoopio;

import java.util.List;

import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.dfs.FormatMatcher;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;

public class HadoopInputFormatEngine<K, V> implements FormatPlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HadoopInputFormatEngine.class);
  
  private final InputFormat<K, V> input;
  private final InputFormat<K, V> input;
  
  public HadoopInputFormatEngine(InputFormat<K, V> input, OutputFormat<K, V> output, boolean exposeKeys){
    this.input = input;
    
  }

  @Override
  public boolean supportsRead() {
    return false;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public FormatMatcher getMatcher() {
    return null;
  }

  @Override
  public AbstractGroupScan getGroupScan(FileSystem fs, List<FileSelection> data) {
    return null;
  }
  
  
  
}
