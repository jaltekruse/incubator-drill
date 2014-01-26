package org.apache.drill.exec.vector.accessor;

import org.apache.drill.exec.vector.ValueVector;

public interface BytesAccessor extends ValueVector.Accessor{
  public byte[] getBytesValue(int index);
}
