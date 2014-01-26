package org.apache.drill.exec.compile;

import org.apache.drill.exec.compile.sig.RuntimeOverridden;

public abstract class ExampleTemplateWithInner implements ExampleInner{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExampleTemplateWithInner.class);
  
  public abstract void doOutside();
  public class TheInnerClass{
    
    @RuntimeOverridden
    public void doInside(){};
    
    
    public void doDouble(){
      DoubleInner di = new DoubleInner();
      di.doDouble();
    }
    
    public class DoubleInner{
      @RuntimeOverridden
      public void doDouble(){};
    }
    
  }
  
  public void doInsideOutside(){
    TheInnerClass inner = new TheInnerClass();
    inner.doInside();
    inner.doDouble();
  }
  
}
