package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.vector.ValueVector;

public interface IntAccessor extends ValueVector.Accessor{
  public int getIntValue(int index);
}
