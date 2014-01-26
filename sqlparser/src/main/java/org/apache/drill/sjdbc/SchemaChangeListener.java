package org.apache.drill.sjdbc;

import org.apache.drill.exec.record.BatchSchema;

public interface SchemaChangeListener {
  public void schemaChanged(BatchSchema newSchema);
}
