package org.apache.drill.exec.vector.accessor;

import java.math.BigDecimal;

import org.apache.drill.exec.vector.ValueVector;

public interface BigDecimalAccessor extends ValueVector.Accessor{
  public BigDecimal getBigDecimalValue(int index);
}
