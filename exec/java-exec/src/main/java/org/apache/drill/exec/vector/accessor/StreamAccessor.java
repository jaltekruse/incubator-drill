package org.apache.drill.exec.vector.accessor;

import java.io.InputStream;

import org.apache.drill.exec.vector.ValueVector;

public interface StreamAccessor extends ValueVector.Accessor{
  public InputStream getStreamValue(int index);
}
