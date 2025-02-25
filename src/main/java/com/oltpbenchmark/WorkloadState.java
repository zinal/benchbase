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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to share a state among the workers of a single workload. Worker use it to ask
 * for work and as interface to the global BenchmarkState
 *
 * @author alendit
 */
public class WorkloadState {
  private static final int RATE_QUEUE_LIMIT = 10000;
  private static final Logger LOG = LoggerFactory.getLogger(WorkloadState.class);

  private final BenchmarkState benchmarkState;
  private final ConcurrentLinkedQueue<SubmittedProcedure> workQueue = new ConcurrentLinkedQueue<>();
  private final int num_terminals;
  private final ReentrantLock guard = new ReentrantLock();
  private final Iterator<Phase> phaseIterator;
  private Phase currentPhase = null;
  private final Semaphore stateSwitchSemaphore = new Semaphore(0);

  private final AtomicInteger workersWaiting = new AtomicInteger(0);
  private final AtomicInteger workersWorking = new AtomicInteger(0);
  private final AtomicInteger workerNeedSleep = new AtomicInteger(0);

  public WorkloadState(BenchmarkState benchmarkState, List<Phase> works, int num_terminals) {
    this.benchmarkState = benchmarkState;
    this.num_terminals = num_terminals;
    this.workerNeedSleep.set(num_terminals);
    this.phaseIterator = works.iterator();
  }

  /** Add a request to do work. */
  public void addToQueue(int amount, boolean resetQueues) {
    int workAdded = 0;

    try {
      guard.lock();
      if (resetQueues) {
        workQueue.clear();
      }

      // Only use the work queue if the phase is enabled and rate limited.
      if (currentPhase == null
          || currentPhase.isDisabled()
          || !currentPhase.isRateLimited()
          || currentPhase.isSerial()) {
        return;
      }

      // Add the specified number of procedures to the end of the queue.
      // If we can't keep up with current rate, truncate transactions
      for (int i = 0; i < amount && workQueue.size() <= RATE_QUEUE_LIMIT; ++i) {
        workQueue.add(new SubmittedProcedure(currentPhase.chooseTransaction()));
        workAdded++;
      }

      // Wake up sleeping workers to deal with the new work.
      int numToWake = Math.min(workAdded, workersWaiting.get());
      stateSwitchSemaphore.release(numToWake);
    } finally {
      guard.unlock();
    }
  }

  public void signalDone() {
    int current = this.benchmarkState.signalDone();
    if (current == 0) {
      // Wake up all waiting threads for shutdown.
      int numWorkers = workersWaiting.get();
      if (numWorkers > 0) {
        stateSwitchSemaphore.release(numWorkers);
      }
    }
  }

  /** Called by ThreadPoolThreads when waiting for work. */
  public SubmittedProcedure fetchWork() {
    final Phase phase = getCurrentPhase();
    if (phase != null && phase.isSerial()) {
      try {
        workersWaiting.incrementAndGet();
        while (getGlobalState() == State.LATENCY_COMPLETE) {
          try {
            stateSwitchSemaphore.acquire();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      } finally {
        workersWaiting.decrementAndGet();
      }

      State state = getGlobalState();
      switch (state) {
        case EXIT, DONE -> {
          return null;
        }
      }

      workersWorking.incrementAndGet();
      return new SubmittedProcedure(currentPhase.chooseTransaction(state == State.COLD_QUERY));
    }

    // Unlimited-rate phases don't use the work queue.
    if (phase != null && !phase.isRateLimited()) {
      workersWorking.incrementAndGet();
      return new SubmittedProcedure(
          currentPhase.chooseTransaction(getGlobalState() == State.COLD_QUERY));
    }

    // Sleep until work is available.
    SubmittedProcedure sp;
    while ((sp = workQueue.poll()) == null) {
      State state = getGlobalState();
      switch (state) {
        case EXIT, DONE -> {
          return null;
        }
      }
      try {
        workersWaiting.incrementAndGet();
        stateSwitchSemaphore.acquire();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        workersWaiting.decrementAndGet();
      }
    }

    workersWorking.incrementAndGet();
    return sp;
  }

  public void finishedWork() {
    workersWorking.decrementAndGet();
  }

  public Phase getNextPhase() {
    if (phaseIterator.hasNext()) {
      return phaseIterator.next();
    }
    return null;
  }

  public Phase getCurrentPhase() {
    try {
      guard.lock();
      return currentPhase;
    } finally {
      guard.unlock();
    }
  }

  /*
   * Called by workers to ask if they should stay awake in this phase
   */
  public void stayAwake() {
    while (workerNeedSleep.get() > 0) {
      if (workerNeedSleep.decrementAndGet() < 0) {
        workerNeedSleep.set(0);
        return;
      }
      try {
        stateSwitchSemaphore.acquire();
      } catch (InterruptedException e) {
        LOG.error("stayAwake() interrupted", e);
      }
    }
  }

  public void switchToNextPhase() {
    try {
      guard.lock();
      this.currentPhase = this.getNextPhase();

      // Clear the work from the previous phase.
      workQueue.clear();

      // Determine how many workers need to sleep, then make sure they do.
      if (this.currentPhase == null) {
        // Benchmark is over---wake everyone up so they can terminate
        workerNeedSleep.set(0);
      } else {
        this.currentPhase.resetSerial();
        if (this.currentPhase.isDisabled()) {
          // Phase disabled---everyone should sleep
          workerNeedSleep.set(num_terminals);
        } else {
          // Phase running---activate the appropriate # of terminals
          workerNeedSleep.set(num_terminals - currentPhase.getActiveTerminals());
        }
      }
    } finally {
      guard.unlock();
    }
  }

  /** Delegates pre-start blocking to the global state handler */
  public void blockForStart() {
    benchmarkState.blockForStart();
  }

  /**
   * Delegates a global state query to the benchmark state handler
   *
   * @return global state
   */
  public State getGlobalState() {
    return benchmarkState.getState();
  }

  public void signalLatencyComplete() {

    benchmarkState.signalLatencyComplete();
  }

  public void startColdQuery() {

    benchmarkState.startColdQuery();
  }

  public void startHotQuery() {

    benchmarkState.startHotQuery();
  }

  public long getTestStartNs() {
    return benchmarkState.getTestStartNs();
  }

  public ReentrantLock getGuard() {
    return guard;
  }
}
