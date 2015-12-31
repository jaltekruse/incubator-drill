/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl.writer;

import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestParquetTypeConversions {

  private static final Chronology UTC = org.joda.time.chrono.ISOChronology.getInstanceUTC();
  private static final String ERR_MSG_DATE_TO_EPOCH_DAYS = "Incorrect conversion between date and \"days since epoch\".";

  private long datetimeToDaysSinceEpoch(long datetime) {
    return DateTimeUtils.toJulianDayNumber(datetime) - ParquetOutputRecordWriter.JULIAN_DAY_EPOC;
  }

  private long fromParquetDateToUnixTimestamp(long daysSinceEpoch) {
    return DateTimeUtils.fromJulianDay(daysSinceEpoch + ParquetOutputRecordWriter.JULIAN_DAY_EPOC - 0.5);
  }

  private long datetimeToDaysSinceEpoch_OLD_INCORRECT_FORMULA(long datetime) {
    return (int) (DateTimeUtils.toJulianDayNumber(datetime) + ParquetOutputRecordWriter.JULIAN_DAY_EPOC);
  }

  private long fromParquetDateToUnixTimestamp_OLD_INCORRECT_FORMULA(long daysSinceEpoch) {
    return DateTimeUtils.fromJulianDay(daysSinceEpoch - ParquetOutputRecordWriter.JULIAN_DAY_EPOC - 0.5);
  }

  @Test
  public void testDate() {
    long datetime = UTC.getDateTimeMillis(1969, 12, 31, 0);
    long daysSinceEpoch = datetimeToDaysSinceEpoch(datetime);
    // this is one day before the Unix epoch 1970-1-1
    assertEquals(ERR_MSG_DATE_TO_EPOCH_DAYS, daysSinceEpoch, -1);

    datetime = UTC.getDateTimeMillis(1970, 1, 2, 0);
    daysSinceEpoch = datetimeToDaysSinceEpoch(datetime);
    // this is one day after the Unix epoch 1970-1-1
    assertEquals(ERR_MSG_DATE_TO_EPOCH_DAYS, daysSinceEpoch, 1);

    long daysSinceEpockConvertedBack = fromParquetDateToUnixTimestamp(daysSinceEpoch);
    // converting back to a datetime give the correct original value
    assertEquals(ERR_MSG_DATE_TO_EPOCH_DAYS, daysSinceEpockConvertedBack, datetime);
  }

  @Test
  public void fixIncorrectOldDate() {
    // taken from JODA, min/max allowed years
    // -292275055, 292278994

    testCorruptDateAutoCorrection(-10000, 1, 1, 0);
    testCorruptDateAutoCorrection(1, 1, 1, 0);
    testCorruptDateAutoCorrection(1900, 1, 1, 0);
    testCorruptDateAutoCorrection(1970, 1, 1, 0);
    testCorruptDateAutoCorrection(2016, 1, 1, 0);
    testCorruptDateAutoCorrection(10000, 1, 1, 0);

    // overflow in correction formula, see if I can simplify the formula to lose less of
    // the range, not sure if trying to convert it back to a timestamp, rather than an exact
    // conversion of the day counts loses any extra possible values in auto-correction
//    testCorruptDateAutoCorrection(-292275055, 1, 1, 0);
//    testCorruptDateAutoCorrection(292278994, 1, 1, 0);

    testCorruptDateAutoCorrection(-292_275_055 + 290_000_000, 1, 1, 0);
    testCorruptDateAutoCorrection(292_278_994 - 290_000_000, 1, 1, 0);
  }

  /**
   * After sending one of the old incorrectly written dates through the fixed
   * parquet reader for reading the standard format, this method determines if
   * for a given date we can recover the original (corrupt) value from the file.
   *
   * @param year
   * @param month
   * @param day
   * @param millisOfDay
   */
  private void testCorruptDateAutoCorrection(int year, int month, int day, int millisOfDay) {
    long datetime = UTC.getDateTimeMillis(year, month, day, millisOfDay);
    // sanity check that the old conversion correctly interprets it's own non-standard format
    assertEquals(datetime,
        fromParquetDateToUnixTimestamp_OLD_INCORRECT_FORMULA(
            datetimeToDaysSinceEpoch_OLD_INCORRECT_FORMULA(datetime)));
    long valueInIncorrectParquetFiles = datetimeToDaysSinceEpoch_OLD_INCORRECT_FORMULA(datetime);
    long resultReadingCorruptValuesWithFixedReader = fromParquetDateToUnixTimestamp(valueInIncorrectParquetFiles);
    long recoveredCorruptValueFromFile = datetimeToDaysSinceEpoch(resultReadingCorruptValuesWithFixedReader);
    assertEquals(recoveredCorruptValueFromFile, valueInIncorrectParquetFiles);

    // actually get the correct date, assuming we have an incorrect value
    assertEquals(fromParquetDateToUnixTimestamp_OLD_INCORRECT_FORMULA(recoveredCorruptValueFromFile), datetime);
  }
}
