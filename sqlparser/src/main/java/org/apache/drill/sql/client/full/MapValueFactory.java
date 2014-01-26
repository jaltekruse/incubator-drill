package org.apache.drill.sql.client.full;

public interface MapValueFactory<KEY, VALUE> {
  
  public VALUE create(KEY key);
  public void destroy(VALUE value);
}
