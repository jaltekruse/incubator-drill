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
package org.apache.drill.exec.server.rest.profile;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DataFormattingHelper {

  private static final NumberFormat format = NumberFormat.getInstance(Locale.US);
  // Here s stands for "short", to avoid providing leading zeros
  private static final SimpleDateFormat daysFormat = new SimpleDateFormat("DD'd'hh'h'mm'm'");
  private static final SimpleDateFormat sdaysFormat = new SimpleDateFormat("D'd'hh'h'mm'm'");
  private static final SimpleDateFormat hoursFormat = new SimpleDateFormat("HH'h'mm'm'");
  private static final SimpleDateFormat shoursFormat = new SimpleDateFormat("H'h'mm'm'");
  private static final SimpleDateFormat minsFormat = new SimpleDateFormat("mm'm'ss's'");
  private static final SimpleDateFormat sminsFormat = new SimpleDateFormat("m'm'ss's'");

  private static final SimpleDateFormat secsFormat = new SimpleDateFormat("ss.SSS's'");
  private static final SimpleDateFormat ssecsFormat = new SimpleDateFormat("s.SSS's'");
  private static final DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
  private static final DecimalFormat dec = new DecimalFormat("0.00");
  private static final DecimalFormat intformat = new DecimalFormat("#,###");

  static {
    format.setMaximumFractionDigits(3);
  }

  public static String formatTime(long time) {
    return dateFormat.format(time);
  }

  public static String formatDuration(long p) {
    final double secs = p/1000.0;
    final double mins = secs/60;
    final double hours = mins/60;
    final double days = hours / 24;
    SimpleDateFormat timeFormat = null;
    if (days >= 10) {
      timeFormat = daysFormat;
    } else if (days >= 1) {
      timeFormat = sdaysFormat;
    } else if (hours >= 10) {
      timeFormat = hoursFormat;
    }else if(hours >= 1){
      timeFormat = shoursFormat;
    }else if (mins >= 10){
      timeFormat = minsFormat;
    }else if (mins >= 1){
      timeFormat = sminsFormat;
    }else if (secs >= 10){
      timeFormat = secsFormat;
    }else {
      timeFormat = ssecsFormat;
    }
    return timeFormat.format(new Date(p));
  }

  public static String formatBinarySize(final long size) {
    final double t = size / Math.pow(1024, 4);
    if (t > 1) {
      return dec.format(t).concat("TB");
    }

    final double g = size / Math.pow(1024, 3);
    if (g > 1) {
      return dec.format(g).concat("GB");
    }

    final double m = size / Math.pow(1024, 2);
    if (m > 1) {
      return intformat.format(m).concat("MB");
    }

    final double k = size / 1024;
    if (k >= 1) {
      return intformat.format(k).concat("KB");
    }

    // size < 1 KB
    return "-";
  }

  public static String formatNumber(final Number n) {
    return format.format(n);
  }

  public static String formatInteger(final long n) {
    return intformat.format(n);
  }

}
