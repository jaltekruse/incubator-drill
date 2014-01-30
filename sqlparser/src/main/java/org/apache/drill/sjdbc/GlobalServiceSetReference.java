package org.apache.drill.sjdbc;

import org.apache.drill.exec.server.RemoteServiceSet;

public class GlobalServiceSetReference {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GlobalServiceSetReference.class);
  
  public static final ThreadLocal<RemoteServiceSet> SETS = new ThreadLocal<RemoteServiceSet>();
  
  
  
}
