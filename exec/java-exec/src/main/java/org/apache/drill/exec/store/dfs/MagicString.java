package org.apache.drill.exec.store.dfs;

public class MagicString {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MagicString.class);
  
  private long offset;
  private byte[] bytes;
  
  public MagicString(long offset, byte[] bytes) {
    super();
    this.offset = offset;
    this.bytes = bytes;
  }

  public long getOffset() {
    return offset;
  }

  public byte[] getBytes() {
    return bytes;
  }
  
  
  
}
