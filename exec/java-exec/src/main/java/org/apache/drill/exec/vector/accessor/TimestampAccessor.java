package org.apache.drill.exec.vector.accessor;

import java.sql.Timestamp;

import org.apache.drill.exec.vector.ValueVector;

public interface TimestampAccessor extends ValueVector.Accessor {
  public Timestamp getTimestampValue(int index);
}
