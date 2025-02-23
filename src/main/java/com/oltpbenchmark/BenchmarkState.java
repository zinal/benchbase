/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark;

import com.oltpbenchmark.types.State;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BenchmarkState {

  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkState.class);

  private final WorkloadConfiguration workloadConf;
  private final long testStartNs;
  private final CountDownLatch startBarrier;
  private final AtomicInteger notDoneCount;
  private final AtomicReference<State> state = new AtomicReference<>(State.WARMUP);

  /**
   * @param numThreads number of threads involved in the test: including the master thread.
   * @param workloadConf workload configuration reference
   */
  public BenchmarkState(int numThreads, WorkloadConfiguration workloadConf) {
    this.workloadConf = workloadConf;
    this.startBarrier = new CountDownLatch(numThreads);
    this.notDoneCount = new AtomicInteger(numThreads);

    this.testStartNs = System.nanoTime();
  }

  // Protected by this

  public long getTestStartNs() {
    return testStartNs;
  }

  public State getState() {
    return state.get();
  }

  /** Wait for all threads to call this. Returns once all the threads have entered. */
  public void blockForStart() {

    startBarrier.countDown();
    try {
      startBarrier.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void startMeasure() {
    state.set(State.MEASURE);
  }

  public void startColdQuery() {
    state.set(State.COLD_QUERY);
  }

  public void startHotQuery() {
    state.set(State.MEASURE);
  }

  public void signalLatencyComplete() {
    state.set(State.LATENCY_COMPLETE);
  }

  public void ackLatencyComplete() {
    state.set(State.MEASURE);
  }

  public void signalError() {
    // A thread died, decrement the count and set error state
    notDoneCount.decrementAndGet();
    state.set(State.ERROR);
  }

  public void startCoolDown() {
    state.set(State.DONE);

    // The master thread must also signal that it is done
    signalDone();
  }

  /** Notify that this thread has entered the done state. */
  public int signalDone() {

    int current = notDoneCount.decrementAndGet();

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("%d workers are not done. Waiting until they finish", current));
    }
    if (current == 0) {
      // We are the last thread to notice that we are done: wake any
      // blocked workers
      state.set(State.EXIT);
    }
    return current;
  }
}
