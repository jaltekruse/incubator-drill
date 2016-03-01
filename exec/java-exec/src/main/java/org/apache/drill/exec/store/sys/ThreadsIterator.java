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
package org.apache.drill.exec.store.sys;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

public class ThreadsIterator implements Iterator<Object> {

  private final FragmentContext context;
  private final Iterator<ThreadInfo> threadInfoIter;

  public ThreadsIterator(final FragmentContext context) {
    this.context = context;
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds());
    threadInfoIter = Arrays.asList(threadInfos).iterator();
  }

  @Override
  public boolean hasNext() {
    return threadInfoIter.hasNext();
  }

  @Override
  public Object next() {
    ThreadInfo currentThread = threadInfoIter.next();
    final ThreadSummary threadSummary = new ThreadSummary();

    final DrillbitEndpoint endpoint = context.getIdentity();
    threadSummary.hostname = endpoint.getAddress();
    threadSummary.user_port = endpoint.getUserPort();
    threadSummary.threadName = currentThread.getThreadName();
//    threadSummary.priority = "HIGH";
    threadSummary.threadState = currentThread.getThreadState().name();
    threadSummary.threadId = currentThread.getThreadId();
    threadSummary.blockedCount = currentThread.getBlockedCount();
    threadSummary.blockedTime = currentThread.getBlockedTime();
    threadSummary.waitedCount = currentThread.getWaitedCount();
    threadSummary.waitedTime = currentThread.getWaitedTime();
    threadSummary.lockName = currentThread.getLockName();
//    threadSummary.lock = currentThread.getLockInfo().toString();
    threadSummary.lockOwnerId = currentThread.getLockOwnerId();
    threadSummary.lockOwnerName = currentThread.getLockOwnerName();
    threadSummary.inNative = currentThread.isInNative();
    threadSummary.suspended = currentThread.isSuspended();

//    threadSummary.total_threads = threadMXBean.getPeakThreadCount();
//    threadSummary.busy_threads = threadMXBean.getThreadCount();
    return threadSummary;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public static class ThreadSummary {
    //name, priority, state, id, thread-level cpu stats

    // should this be in all distributed system tables?
    public String hostname;
    public long user_port;

    // pulled out of java.lang.management.ThreadInfo
    public String       threadName;
    public long         threadId;
    public long         blockedTime;
    public long         blockedCount;
    public long         waitedTime;
    public long         waitedCount;
//    public LockInfo     lock;
    public String       lock;
    public String       lockName;
    public long         lockOwnerId;
    public String       lockOwnerName;
    public boolean      inNative;
    public boolean      suspended;
//    public Thread.State threadState;
    public String       threadState;

//    public StackTraceElement[] stackTrace;
//    public MonitorInfo[]       lockedMonitors;
//    public LockInfo[]          lockedSynchronizers;

//    public static MonitorInfo[] EMPTY_MONITORS = new MonitorInfo[0];
//    public static LockInfo[] EMPTY_SYNCS = new LockInfo[0];

    // TODO - Not sure about this one
//    public String priority;

    // TODO - remove? from old threads table
//    public long total_threads;
//    public long busy_threads;
  }
}
