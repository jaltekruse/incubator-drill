package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.vector.ValueVector;

public interface DoubleAccessor extends ValueVector.Accessor {
  public double getDoubleValue(int index);
}
