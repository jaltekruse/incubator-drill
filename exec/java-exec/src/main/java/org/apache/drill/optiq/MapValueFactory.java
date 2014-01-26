package org.apache.drill.optiq;

public interface MapValueFactory<KEY, VALUE> {
  
  public VALUE create(KEY key);
  public void destroy(VALUE value);
}
