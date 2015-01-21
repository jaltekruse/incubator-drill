/*
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
package org.apache.drill.exec.physical.impl;

import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Accountor for tracking completion of sending of items (e.g., messages,
 * batches).
 * <p>
 *   Counts start and completion of sending of items and supports waiting for
 *   completion of all start items.
 * </p>
 * <p>
 *   (It is necessary to wait for completion of sending message before
 *   finishing a task so we don't think buffers are hanging (leaked) when they
 *   will be released once sending completes.)
 * </p>
 * <p>
 *   TODO: Need to update to use long for number of pending messages.
 * <p>
 */
public class SendingAccountor {
  static final Logger logger = getLogger(SendingAccountor.class);

  /** Number of items started (per {@link #increment} calls) but not yet
      waited for (via {@link #waitForSendComplete}). */
  private int unawaitedStartedItemsCount = 0;

  /** Effectively, number of completed items not yet waited for.
      The number of available permits is the number of completed items
      (per {@link# decrement()} calls) minus the number of items already waited
      for (via {@link #waitForSendComplete}). */
  private Semaphore unwaitedCompletedItemsCount = new Semaphore(0);

  // Note:  This needs to be synchronized because this and waitForSendComplete()
  // both access the not-yet-waited-for started items count (or documentation
  // must specify that this and waitForSendComplete() must not be called
  // concurrently).  (The count can't just be volatile or atomic because is it
  // both read and written by each method.)
  /**
   * Counts the start of sending of an item.
   * <p> Thread-safe. </p>
   */
  public synchronized void increment() {
    unawaitedStartedItemsCount++;
  }

  /**
   * Counts the completion of sending of an item.
   * <p> Thread-safe. </p>
   */
  public void decrement() {
    unwaitedCompletedItemsCount.release();
  }

  /**
   * Waits for completion of sending of any pending items.
   * <p>
   *   Waits for completion of sending (per {@link #decrement()} calls) of any
   *   started items (per {@link #increment()} calls) that have not already
   *   been waited for (via this method).  (Doesn't wait if no such items.)
   * </p>
   * <p> Thread-safe. </p>
   */
  public synchronized void waitForSendComplete() {
    try {
      unwaitedCompletedItemsCount.acquire(unawaitedStartedItemsCount);
      unawaitedStartedItemsCount = 0;
    } catch (InterruptedException e) {
      logger.warn("Interruption while waiting for send to complete.", e);
      // TODO:  Review:  Shouldn't this call Thread.currentThread().interrupt()
      // to re-set the interrupt bit ?
    }
  }

}
