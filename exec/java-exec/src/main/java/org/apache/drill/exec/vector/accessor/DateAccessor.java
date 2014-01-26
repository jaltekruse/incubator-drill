package org.apache.drill.exec.vector.accessor;

import java.sql.Date;

import org.apache.drill.exec.vector.ValueVector;

public interface DateAccessor extends ValueVector.Accessor {
  public Date getDateValue(int index);
}
