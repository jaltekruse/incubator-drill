package org.apache.drill.client.jdbc;

import java.util.Calendar;
import java.util.List;

import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.Cursor;

public class DrillCursor implements Cursor{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillCursor.class);

  @Override
  public List<Accessor> createAccessors(List<ColumnMetaData> types, Calendar localCalendar) {
    return null;
  }

  @Override
  public boolean next() {
    return false;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean wasNull() {
    return false;
  }
}
