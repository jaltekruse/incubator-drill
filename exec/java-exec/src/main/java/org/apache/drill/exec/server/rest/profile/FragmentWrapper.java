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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.drill.exec.proto.UserBitShared.MajorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

/**
 * Wrapper class for a major fragment profile.
 */
public class FragmentWrapper {
  private final MajorFragmentProfile major;
  private final long start;
  private final long firstStart;
  private final long lastStart;
  private final String operatorPath;
  private final long numberRunningFragments;
  private final String minorFragmentsReporting;
  private final long firstEnd;
  private final long lastEnd;
  private final long shortRunTime;
  private final long longRunTime;
  private final long avgRunTime;
  private final long lastProgress;
  private final long lastUpdate;
  private final long maxMem;

  public FragmentWrapper(final MajorFragmentProfile major, final long start) {
    this.major = Preconditions.checkNotNull(major);
    this.start = start;

    // Use only minor fragments that have complete profiles
    // Complete iff the fragment profile has at least one operator profile, and start and end times.
    final List<MinorFragmentProfile> complete = new ArrayList<>(
        Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));
    operatorPath = new OperatorPathBuilder().setMajor(major).build();
    numberRunningFragments = major.getMinorFragmentProfileCount() - complete.size();
    minorFragmentsReporting = complete.size() + " / " + major.getMinorFragmentProfileCount();

    firstStart = Collections.min(complete, Comparators.startTime).getStartTime() - start;
    lastStart = Collections.max(complete, Comparators.startTime).getStartTime() - start;

    firstEnd = Collections.min(complete, Comparators.endTime).getEndTime() - start;
    lastEnd = Collections.max(complete, Comparators.endTime).getEndTime() - start;

    long total = 0;
    for (final MinorFragmentProfile p : complete) {
      total += p.getEndTime() - p.getStartTime();
    }

    final MinorFragmentProfile shortRun = Collections.min(complete, Comparators.runTime);
    shortRunTime = shortRun.getEndTime() - shortRun.getStartTime();
    final MinorFragmentProfile longRun = Collections.max(complete, Comparators.runTime);
    longRunTime = longRun.getEndTime() - longRun.getStartTime();
    avgRunTime = total / complete.size();
    lastUpdate = Collections.max(complete, Comparators.lastUpdate).getLastUpdate();
    lastProgress = Collections.max(complete, Comparators.lastProgress).getLastProgress();

    // TODO(DRILL-3494): Names (maxMem, getMaxMemoryUsed) are misleading; the value is peak memory allocated to fragment
    maxMem = Collections.max(complete, Comparators.fragmentPeakMemory).getMaxMemoryUsed();
  }

  public String getDisplayName() {
    return String.format("Major Fragment: %s", new OperatorPathBuilder().setMajor(major).build());
  }

  public String getId() {
    return String.format("fragment-%s", major.getMajorFragmentId());
  }

  public static final String[] FRAGMENT_OVERVIEW_COLUMNS = {"Major Fragment", "Minor Fragments Reporting",
    "First Start", "Last Start", "First End", "Last End", "Min Runtime", "Avg Runtime", "Max Runtime", "Last Update",
    "Last Progress", "Max Peak Memory"};

  // Not including Major Fragment ID and Minor Fragments Reporting
  public static final int NUM_NULLABLE_OVERVIEW_COLUMNS = FRAGMENT_OVERVIEW_COLUMNS.length - 2;

  public void addSummary(TableBuilder tb) {
    // Use only minor fragments that have complete profiles
    // Complete iff the fragment profile has at least one operator profile, and start and end times.
    final List<MinorFragmentProfile> complete = new ArrayList<>(
      Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));

    tb.appendCell(operatorPath, null);
    tb.appendCell(minorFragmentsReporting, null);

    // If there are no stats to aggregate, create an empty row
    if (complete.size() < 1) {
      tb.appendRepeated("", null, NUM_NULLABLE_OVERVIEW_COLUMNS);
      return;
    } else {
      tb.appendCells(
          DataFormattingHelper.formatDuration(firstStart),
          DataFormattingHelper.formatDuration(lastStart),
          DataFormattingHelper.formatDuration(firstEnd),
          DataFormattingHelper.formatDuration(lastEnd),
          DataFormattingHelper.formatDuration(shortRunTime),
          DataFormattingHelper.formatDuration(avgRunTime),
          DataFormattingHelper.formatDuration(longRunTime),
          DataFormattingHelper.formatTime(lastUpdate),
          DataFormattingHelper.formatTime(lastProgress),
          DataFormattingHelper.formatBinarySize(maxMem)
      );
    }
  }

  public static final String[] FRAGMENT_COLUMNS = {"Minor Fragment ID", "Host Name", "Start", "End",
    "Runtime", "Max Records", "Max Batches", "Last Update", "Last Progress", "Peak Memory", "State"};

  // Not including minor fragment ID
  private static final int NUM_NULLABLE_FRAGMENTS_COLUMNS = FRAGMENT_COLUMNS.length - 1;

  public String getContent() {
    final TableBuilder builder = new TableBuilder(FRAGMENT_COLUMNS);

    // Use only minor fragments that have complete profiles
    // Complete iff the fragment profile has at least one operator profile, and start and end times.
    final List<MinorFragmentProfile> complete = new ArrayList<>(
      Collections2.filter(major.getMinorFragmentProfileList(), Filters.hasOperatorsAndTimes));
    final List<MinorFragmentProfile> incomplete = new ArrayList<>(
      Collections2.filter(major.getMinorFragmentProfileList(), Filters.missingOperatorsOrTimes));

    Collections.sort(complete, Comparators.minorId);
    for (final MinorFragmentProfile minor : complete) {
      MinorFragmentWrapper fragmentWrapper = new MinorFragmentWrapper(major, start, minor);

      builder.appendCells(
          fragmentWrapper.minorFragmentID,
          fragmentWrapper.hostName,
          DataFormattingHelper.formatDuration(fragmentWrapper.startTime),
          DataFormattingHelper.formatDuration(fragmentWrapper.endTime),
          DataFormattingHelper.formatDuration(fragmentWrapper.runTime),
          DataFormattingHelper.formatInteger(fragmentWrapper.maxRecords),
          DataFormattingHelper.formatInteger(fragmentWrapper.maxBatches),
          DataFormattingHelper.formatInteger(fragmentWrapper.maxBatches),
          DataFormattingHelper.formatTime(fragmentWrapper.lastProgress),
          DataFormattingHelper.formatTime(fragmentWrapper.lastUpdate),
          DataFormattingHelper.formatBinarySize(fragmentWrapper.maxMem),
          fragmentWrapper.state
      );
    }

    for (final MinorFragmentProfile m : incomplete) {
      builder.appendCell(major.getMajorFragmentId() + "-" + m.getMinorFragmentId(), null);
      builder.appendRepeated(m.getState().toString(), null, NUM_NULLABLE_FRAGMENTS_COLUMNS);
    }
    return builder.build();
  }

  public long getNumberRunningFragments() {
    return numberRunningFragments;
  }

  public static class MinorFragmentWrapper {
    private final String minorFragmentID;
    private final String hostName;
    private final long startTime;
    private final long endTime;
    private final long runTime;
    private final long maxRecords;
    private final long maxBatches;
    private final long lastProgress;
    private final long lastUpdate;
    private final long maxMem;
    private final String state;

    public MinorFragmentWrapper(final MajorFragmentProfile major, final long start, final MinorFragmentProfile minor) {
      final ArrayList<OperatorProfile> ops = new ArrayList<>(minor.getOperatorProfileList());

      long biggestIncomingRecords = 0;
      long biggestBatches = 0;
      for (final OperatorProfile op : ops) {
        long incomingRecords = 0;
        long batches = 0;
        for (final StreamProfile sp : op.getInputProfileList()) {
          incomingRecords += sp.getRecords();
          batches += sp.getBatches();
        }
        biggestIncomingRecords = Math.max(biggestIncomingRecords, incomingRecords);
        biggestBatches = Math.max(biggestBatches, batches);
      }

      minorFragmentID = new OperatorPathBuilder().setMajor(major).setMinor(minor).build();
      hostName = minor.getEndpoint().getAddress();
      startTime = minor.getStartTime() - start;
      endTime = minor.getEndTime() - start;
      runTime = minor.getEndTime() - minor.getStartTime();

      maxRecords = biggestIncomingRecords;
      maxBatches = biggestBatches;

      lastUpdate = minor.getLastUpdate();
      lastProgress = minor.getLastProgress();

      maxMem = minor.getMaxMemoryUsed();
      state = minor.getState().name();
    }
  }
}
