package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.vector.ValueVector;

public interface ByteAccessor extends ValueVector.Accessor {
  public byte getByteValue(int index);
}
