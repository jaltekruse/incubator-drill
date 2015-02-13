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
package org.apache.drill.exec.memory;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.UnsafeDirectLittleEndian;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.drill.common.StackTrace;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.util.AssertionUtil;
import org.apache.drill.exec.util.Pointer;

@Deprecated // use RootAllocator instead
public class TopLevelAllocator implements BufferAllocator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TopLevelAllocator.class);

  public static long MAXIMUM_DIRECT_MEMORY;

  private static final boolean ENABLE_ACCOUNTING = AssertionUtil.isAssertionsEnabled();
  private final Map<ChildAllocator, StackTraceElement[]> childrenMap;
  private final PooledByteBufAllocatorL innerAllocator = PooledByteBufAllocatorL.DEFAULT;
  private final Accountor acct;
  private final boolean errorOnLeak;
  private final DrillBuf empty;

  // Constructor made private to prevent any further use of this class before it is removed.
  // Use RootAllocator instead.
  @Deprecated
  private TopLevelAllocator(final DrillConfig config) {
    errorOnLeak = config.getBoolean(ExecConstants.ERROR_ON_MEMORY_LEAK);
    final long maximumAllocation =
        Math.min(DrillConfig.getMaxDirectMemory(), config.getLong(ExecConstants.TOP_LEVEL_MAX_ALLOC));
    acct = new Accountor(config, errorOnLeak, null, null, maximumAllocation, 0, true);
    childrenMap = ENABLE_ACCOUNTING ? new IdentityHashMap<ChildAllocator, StackTraceElement[]>() : null;
    empty = DrillBuf.getEmpty(this, acct);
  }

  @Override
  public boolean takeOwnership(DrillBuf buf) {
    return buf.transferAccounting(acct);
  }

  @Override
  public boolean shareOwnership(final DrillBuf buf, final Pointer<DrillBuf> out) {
    final DrillBuf b = new DrillBuf(this, acct, buf);
    out.value = b;
    return acct.transferIn(b, b.capacity());
  }

  @Override
  public DrillBuf buffer(int min, int max) {
    if (min == 0) {
      return empty;
    }
    if(!acct.reserve(min)) {
      return null;
    }
    UnsafeDirectLittleEndian buffer = innerAllocator.directBuffer(min, max);
    DrillBuf wrapped = new DrillBuf(this, acct, buffer);
    acct.reserved(min, wrapped);
    return wrapped;
  }

  @Override
  public DrillBuf buffer(final int size) {
    return buffer(size, size);
  }

  @Override
  public long getAllocatedMemory() {
    return acct.getAllocation();
  }

  @Override
  public long getPeakMemoryAllocation() {
    return acct.getPeakMemoryAllocation();
  }

  @Override
  public ByteBufAllocator getUnderlyingAllocator() {
    return innerAllocator;
  }

  @Override
  public BufferAllocator getChildAllocator(final FragmentContext context, final long initialReservation,
      final long maximumReservation, final boolean applyFragmentLimit) {
    if (!acct.reserve(initialReservation)) {
      final String message = String.format("You attempted to create a new child allocator with initial "
          + "reservation %d but only %d bytes of memory were available.",
          initialReservation, acct.getCapacity() - acct.getAllocation());
      logger.debug(message);
      throw new OutOfMemoryRuntimeException(message);
    }

    logger.debug("New child allocator with initial reservation {}", initialReservation);
    final ChildAllocator allocator =
        new ChildAllocator(context, acct, maximumReservation, initialReservation, childrenMap, applyFragmentLimit);

    if(ENABLE_ACCOUNTING) {
      childrenMap.put(allocator, Thread.currentThread().getStackTrace());
    }

    return allocator;
  }

  @Deprecated
  public void resetFragmentLimits() {
    acct.resetFragmentLimits();
  }

  @Override
  public void setFragmentLimit(final long limit) {
    acct.setFragmentLimit(limit);
  }

  @Override
  public long getFragmentLimit() {
    return acct.getFragmentLimit();
  }

  @Override
  public void close() throws Exception {
    if (ENABLE_ACCOUNTING) {
      for (Entry<ChildAllocator, StackTraceElement[]> child : childrenMap.entrySet()) {
        if (!child.getKey().isClosed()) {
          final StringBuilder sb = new StringBuilder();
          final StackTraceElement[] elements = child.getValue();
          for (int i = 0; i < elements.length; i++) {
            sb.append("\t\t");
            sb.append(elements[i]);
            sb.append("\n");
          }
          throw new IllegalStateException("Failure while trying to close allocator: Child level allocators not closed. Stack trace: \n" + sb);
        }
      }
    }
    acct.close();
  }



  @Override
  public DrillBuf getEmpty() {
    return empty;
  }

  private class ChildAllocator implements BufferAllocator {
//    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ChildAllocator.class);

    private final DrillBuf empty;
    private final Accountor childAcct;
    private final Map<ChildAllocator, StackTraceElement[]> children = new HashMap<>();
    private boolean closed = false;
    private final FragmentHandle handle;
    private final FragmentContext fragmentContext;
    private final Map<ChildAllocator, StackTraceElement[]> thisMap;
    private final boolean applyFragmentLimit;

    public ChildAllocator(final FragmentContext context,
                          final Accountor parentAccountor,
                          final long max,
                          final long pre,
                          final Map<ChildAllocator, StackTraceElement[]> map,
                          final boolean applyFragmentLimit) {
      assert max >= pre;
      this.applyFragmentLimit = applyFragmentLimit;
      childAcct = new Accountor(context.getConfig(), errorOnLeak, context, parentAccountor, max, pre, applyFragmentLimit);
      this.fragmentContext = context;
      this.handle = context.getHandle();
      thisMap = map;
      empty = DrillBuf.getEmpty(this, childAcct);
    }

    @Override
    public boolean takeOwnership(DrillBuf buf) {
      return buf.transferAccounting(acct);
    }

    @Override
    public boolean shareOwnership(final DrillBuf buf, final Pointer<DrillBuf> out) {
      final DrillBuf b = new DrillBuf(this, acct, buf);
      out.value = b;
      return acct.transferIn(b, b.capacity());
    }

    @Override
    public DrillBuf buffer(final int size, final int max) {
      if (size == 0) {
        return empty;
      }
      if(!childAcct.reserve(size)) {
        logger.warn("Unable to allocate buffer of size {} due to memory limit. Current allocation: {}", size, getAllocatedMemory(), new Exception());
        return null;
      };

      final UnsafeDirectLittleEndian buffer = innerAllocator.directBuffer(size, max);
      final DrillBuf wrapped = new DrillBuf(this, childAcct, buffer);
      childAcct.reserved(buffer.capacity(), wrapped);
      return wrapped;
    }

    @Override
    public DrillBuf buffer(final int size) {
      return buffer(size, size);
    }

    @Override
    public ByteBufAllocator getUnderlyingAllocator() {
      return innerAllocator;
    }

    @Override
    public BufferAllocator getChildAllocator(final FragmentContext context,
        final long initialReservation, final long maximumReservation, final boolean applyFragmentLimit) {
      if (!childAcct.reserve(initialReservation)) {
        throw new OutOfMemoryRuntimeException(
            String.format("You attempted to create a new child allocator with initial reservation %d but only %d bytes of memory were available.", initialReservation, childAcct.getAvailable()));
      };
      logger.debug("New child allocator with initial reservation {}", initialReservation);
      final ChildAllocator newChildAllocator =
          new ChildAllocator(context, childAcct, maximumReservation, initialReservation, null, applyFragmentLimit);
      children.put(newChildAllocator, Thread.currentThread().getStackTrace());
      return newChildAllocator;
    }

    @Override
    public AllocationReservation newReservation() {
      return new Reservation(this, acct);
    }

    @Deprecated
    public void resetFragmentLimits() {
      childAcct.resetFragmentLimits();
    }

    @Override
    public void setFragmentLimit(final long limit) {
      childAcct.setFragmentLimit(limit);
    }

    @Override
    public long getFragmentLimit() {
      return childAcct.getFragmentLimit();
    }

    @Override
    public void close() throws Exception {
      if (ENABLE_ACCOUNTING) {
        if (thisMap != null) {
          thisMap.remove(this);
        }
        for (ChildAllocator child : children.keySet()) {
          if (!child.isClosed()) {
            final StringBuilder sb = new StringBuilder();
            final StackTraceElement[] elements = children.get(child);
            for (int i = 3; i < elements.length; i++) {
              sb.append("\t\t");
              sb.append(elements[i]);
              sb.append("\n");
            }

            final IllegalStateException e = new IllegalStateException(String.format(
                    "Failure while trying to close child allocator: Child level allocators not closed. Fragment %d:%d. Stack trace: \n %s",
                    handle.getMajorFragmentId(), handle.getMinorFragmentId(), sb.toString()));
            if (errorOnLeak) {
              throw e;
            } else {
              logger.warn("Memory leak.", e);
            }
          }
        }
      }
      childAcct.close();
      closed = true;
    }

    public boolean isClosed() {
      return closed;
    }

    @Override
    public long getAllocatedMemory() {
      return childAcct.getAllocation();
    }

    @Override
    public long getPeakMemoryAllocation() {
      return childAcct.getPeakMemoryAllocation();
    }

    @Override
    public DrillBuf getEmpty() {
      return empty;
    }

    @Override
    public BufferAllocator newChildAllocator(AllocatorOwner allocatorOwner,
        long initialReservation, long maxAllocation, int flags) {
      return getChildAllocator(null, initialReservation, maxAllocation, (flags & F_LIMITING_ROOT) != 0);
    }

    @Override
    public int getId() {
      throw new UnsupportedOperationException("unimplemented:TopLevelAllocator.ChildAllocator.getId()");
    }
  }

  @Override
  public AllocationReservation newReservation() {
    return new Reservation(this, acct);
  }

  private static class Reservation extends AllocationReservation {
    final BufferAllocator allocator;
    final Accountor accountor;

    Reservation(final BufferAllocator allocator, final Accountor accountor) {
      this.allocator = allocator;
      this.accountor = accountor;
    }

    @Override
    protected boolean reserve(final int nBytes) {
      return accountor.reserve(nBytes);
    }

    @Override
    protected DrillBuf allocate(final int nBytes) {
      return allocator.buffer(nBytes);
    }

    @Override
    protected void releaseReservation(final int nBytes) {
      accountor.release(null, nBytes);
    }
  }

  @Override
  public BufferAllocator newChildAllocator(AllocatorOwner allocatorOwner,
      long initialReservation, long maxAllocation, int flags) {
    return getChildAllocator(null, initialReservation, maxAllocation, (flags & F_LIMITING_ROOT) != 0);
  }

  @Override
  public int getId() {
    throw new UnsupportedOperationException("unimplemented:TopLevelAllocator.getId()");
  }
}
