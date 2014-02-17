package org.apache.drill.exec.planner.logical;

import org.apache.drill.common.logical.StorageEngineConfig;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

public class DynamicDrillTable extends DrillTable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamicDrillTable.class);

  public DynamicDrillTable(String storageEngineName, Object selection, StorageEngineConfig storageEngineConfig) {
    super(storageEngineName, selection, storageEngineConfig);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return new RelDataTypeDrillImpl(typeFactory);
  }
}
