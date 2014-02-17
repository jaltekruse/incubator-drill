package org.apache.drill.exec.store.dfs.shim;

import java.io.InputStream;

public abstract class DrillInputStream implements AutoCloseable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillInputStream.class);

//  public abstract AccountingByteBuf readNow(long start, long length) throws IOException;
//  public abstract void readNow(AccountingByteBuf b, long start, long length) throws IOException;
//  public abstract AccountingByteBuf readNow() throws IOException;
  
  public abstract InputStream getInputStream();
//  public abstract CheckedFuture<Long, IOException> readFuture(AccountingByteBuf b, long start, long length) throws IOException;
  
}
