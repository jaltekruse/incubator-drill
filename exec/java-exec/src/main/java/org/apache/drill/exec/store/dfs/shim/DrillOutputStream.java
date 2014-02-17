package org.apache.drill.exec.store.dfs.shim;

import java.io.OutputStream;


public abstract class DrillOutputStream implements AutoCloseable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillOutputStream.class);

  public abstract OutputStream getOuputStream();
//  public abstract CheckedFuture<Long, IOException> writeFuture(AccountingByteBuf b);
  
}
