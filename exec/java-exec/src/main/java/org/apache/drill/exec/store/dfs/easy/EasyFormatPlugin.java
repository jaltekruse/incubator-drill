package org.apache.drill.exec.store.dfs.easy;

import java.util.List;

import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.dfs.DirectoryTree;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class EasyFormatPlugin implements FormatPlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyFormatPlugin.class);

  /**
   * Whether or not you can split the format based on blocks within file boundaries. If not, the simple format engine will
   * only split on file boundaries.
   * 
   * @return True if splittable.
   */
  public abstract boolean isBlockSplittable();

  public abstract RecordReader getRecordReader(DirectoryTree tree, Path file, long startOffset, long length);

  @Override
  public SimpleFormatGroupScan getGroupScan(FileSystem fs, List<FileSelection> data) {
    SimpleFormatGroupScan scan = new SimpleFormatGroupScan();
    
    
  }

  
}
