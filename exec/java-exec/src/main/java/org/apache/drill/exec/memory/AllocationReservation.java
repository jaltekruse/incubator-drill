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

import io.netty.buffer.DrillBuf;

/**
 * Supports cumulative allocation reservation. Clients may increase the size of
 * the reservation repeatedly until they call for an allocation of the current
 * total size. The reservation can only be used once, and will throw an exception
 * if it is used more than once.
 *
 * <p>For the purposes of airtight memory accounting, the reservation must be close()d
 * whether it is used or not.
 */
public abstract class AllocationReservation implements AutoCloseable {
  private int nBytes = 0;
  private boolean used = false;
  private boolean closed = false;

  /**
   * Constructor. Prevent construction except by derived classes.
   *
   * <p>The expectation is that the derived class will be a non-static inner
   * class in an allocator.
   */
  protected AllocationReservation() {
  }

  /**
   * Add to the current reservation.
   *
   * <p>Adding may fail if the allocator is not allowed to consume any more space.
   *
   * @param nBytes the number of bytes to add
   * @return true if the addition is possible, false otherwise
   * @throws IllegalStateException if called after buffer() is used to allocate the reservation
   */
  public boolean add(final int nBytes) {
    if (closed) {
      throw new IllegalStateException("Attempt to increase reservation after reservation has been closed");
    }
    if (used) {
      throw new IllegalStateException("Attempt to increase reservation after reservation has been used");
    }

    if (!reserve(nBytes)) {
      return false;
    }

    this.nBytes += nBytes;
    return true;
  }

  /**
   * Requests a reservation of additional space.
   *
   * <p>The implementation of the allocator's inner class provides this.
   *
   * @param nBytes the amount to reserve
   * @return true if the reservation can be satisfied, false otherwise
   */
  protected abstract boolean reserve(int nBytes);

  /**
   * Allocate a buffer whose size is the total of all the add()s made.
   *
   * <p>The allocation request can still fail, even if the amount of space
   * requested is available, if the allocation cannot be made contiguously.
   *
   * @return the buffer, or null, if the request cannot be satisfied
   * @throws IllegalStateException if called called more than once
   */
  public DrillBuf buffer() {
    if (closed) {
      throw new IllegalStateException("Attempt to allocate after closed");
    }
    if (used) {
      throw new IllegalStateException("Attempt to allocate more than once");
    }

    final DrillBuf drillBuf = allocate(nBytes);
    used = true;
    return drillBuf;
  }

  /**
   * Allocate the a buffer of the requested size.
   *
   * <p>The implementation of the allocator's inner class provides this.
   *
   * @param nBytes the size of the buffer requested
   * @return the buffer, or null, if the request cannot be satisfied
   */
  protected abstract DrillBuf allocate(int nBytes);

  @Override
  public void close() throws Exception {
    if (closed) {
      throw new IllegalStateException("Reservation has already been closed");
    }
    if (!used) {
      releaseReservation(nBytes);
    }

    closed = true;
  }

  /**
   * Return the reservation back to the allocator without having used it.
   *
   * @param nBytes the size of the reservation
   */
  protected abstract void releaseReservation(int nBytes);
}
