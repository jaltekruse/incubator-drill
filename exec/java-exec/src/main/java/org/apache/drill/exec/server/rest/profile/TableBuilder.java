/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server.rest.profile;

class TableBuilder {

  private StringBuilder sb;
  private int w = 0;
  private int width;

  public TableBuilder(final String[] columns) {
    sb = new StringBuilder();
    width = columns.length;
    sb.append("<table class=\"table table-bordered text-right\">\n<tr>");
    for (final String cn : columns) {
      sb.append("<th>" + cn + "</th>");
    }
    sb.append("</tr>\n");
  }

  public void appendCell(final String s, final String link) {
    if (w == 0) {
      sb.append("<tr>");
    }
    sb.append(String.format("<td>%s%s</td>", s, link != null ? link : ""));
    if (++w >= width) {
      sb.append("</tr>\n");
      w = 0;
    }
  }

  public void appendRepeated(final String s, final String link, final int n) {
    for (int i = 0; i < n; i++) {
      appendCell(s, link);
    }
  }
  public void appendCells(String... cells) {
    for (String s : cells) {
      appendCell(s, null);
    }
  }

  public void appendTime(final long d, final String link) {
    appendCell(DataFormattingHelper.formatTime(d), link);
  }

  public void appendMillis(final long p) {
    appendCell(DataFormattingHelper.formatDuration(p), null);
  }

  public void appendNanos(final long p, String link) {
    appendMillis(Math.round(p / 1000.0 / 1000.0));
  }

  public void appendFormattedNumber(final Number n, final String link) {
    appendCell(DataFormattingHelper.formatNumber(n), link);
  }

  public void appendFormattedInteger(final long n, final String link) {
    appendCell(DataFormattingHelper.formatInteger(n), link);
  }

  public void appendInteger(final long l, final String link) {
    appendCell(Long.toString(l), link);
  }

  public void appendBytes(final long l, final String link){
    appendCell(DataFormattingHelper.formatBinarySize(l), link);
  }

  public String build() {
    String rv;
    rv = sb.append("\n</table>").toString();
    sb = null;
    return rv;
  }
}