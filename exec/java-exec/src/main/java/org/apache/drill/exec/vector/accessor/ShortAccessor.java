package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.vector.ValueVector;

public interface ShortAccessor extends ValueVector.Accessor {
  public short getShortValue(int index);
}
