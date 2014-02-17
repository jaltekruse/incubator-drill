package org.apache.drill.exec.store.dfs;

import java.io.IOException;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;

/**
 * Similar to a storage engine but built specifically to work within a FileSystem context.
 */
public interface FormatPlugin {

  public boolean supportsRead();

  public boolean supportsWrite();
  
  public FormatMatcher getMatcher();

  public AbstractGroupScan getGroupScan(FieldReference outputRef, ReadHandle handle) throws IOException;
  
  public StorageEngineConfig getConfig();
}
