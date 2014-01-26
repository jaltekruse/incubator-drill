package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.vector.ValueVector;

public interface BooleanAccessor extends ValueVector.Accessor{
  public boolean getBooleanValue(int index);
}
