package org.apache.drill.exec.vector.accessor;

import java.sql.Time;

import org.apache.drill.exec.vector.ValueVector;

public interface TimeAccessor extends ValueVector.Accessor {
  public Time getTimeValue(int index);
}
