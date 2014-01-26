package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.vector.ValueVector;

public interface LongAccessor extends ValueVector.Accessor {
  public long getLongValue(int index);
}
