package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.vector.ValueVector;

public interface FloatAccessor extends ValueVector.Accessor {
  public float getFloatValue(int index);
}
