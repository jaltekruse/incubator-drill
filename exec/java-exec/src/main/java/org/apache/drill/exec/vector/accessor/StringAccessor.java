package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.vector.ValueVector;

public interface StringAccessor extends ValueVector.Accessor {
  public String getStringValue(int index);
}
