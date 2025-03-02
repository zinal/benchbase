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

package com.oltpbenchmark.api;

import com.oltpbenchmark.*;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.Histogram;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Worker<T extends BenchmarkModule> implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
  private static final Logger ABORT_LOG =
      LoggerFactory.getLogger("com.oltpbenchmark.api.ABORT_LOG");

  private WorkloadState workloadState;
  private ResultStats resultStats;

  // Interval requests used by the monitor
  private final AtomicInteger intervalRequests = new AtomicInteger(0);

  private final int id;
  private final T benchmark;
  protected final WorkloadConfiguration configuration;
  protected final TransactionTypes transactionTypes;
  protected final Map<TransactionType, Procedure> procedures = new HashMap<>();
  protected final Map<String, Procedure> name_procedures = new HashMap<>();
  protected final Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<>();

  private final Histogram<TransactionType> txnUnknown = new Histogram<>();
  private final Histogram<TransactionType> txnSuccess = new Histogram<>();
  private final Histogram<TransactionType> txnAbort = new Histogram<>();
  private final Histogram<TransactionType> txnRetry = new Histogram<>();
  private final Histogram<TransactionType> txnErrors = new Histogram<>();
  private final Histogram<TransactionType> txtRetryDifferent = new Histogram<>();

  private boolean seenDone = false;

  public Worker(T benchmark, int id) {
    this.id = id;
    this.benchmark = benchmark;
    this.configuration = this.benchmark.getWorkloadConfiguration();
    this.workloadState = this.configuration.getWorkloadState();
    this.transactionTypes = this.configuration.getTransTypes();
    this.resultStats = new ResultStats(this.transactionTypes);

    // Generate all the Procedures that we're going to need
    this.procedures.putAll(this.benchmark.getProcedures());
    for (Entry<TransactionType, Procedure> e : this.procedures.entrySet()) {
      Procedure proc = e.getValue();
      this.name_procedures.put(e.getKey().getName(), proc);
      this.class_procedures.put(proc.getClass(), proc);
    }
  }

  /** Get the BenchmarkModule managing this Worker */
  public final T getBenchmark() {
    return (this.benchmark);
  }

  /** Get the unique thread id for this worker */
  public final int getId() {
    return this.id;
  }

  @Override
  public String toString() {
    return String.format("%s<%03d>", this.getClass().getSimpleName(), this.getId());
  }

  public final WorkloadConfiguration getWorkloadConfiguration() {
    return (this.benchmark.getWorkloadConfiguration());
  }

  public final Random rng() {
    return (this.benchmark.rng());
  }

  public ResultStats getStats() {
    return resultStats;
  }

  public final long getRequests() {
    return resultStats.count();
  }

  public final int getAndResetIntervalRequests() {
    return intervalRequests.getAndSet(0);
  }

  public final Procedure getProcedure(TransactionType type) {
    return (this.procedures.get(type));
  }

  @SuppressWarnings("unchecked")
  public final <P extends Procedure> P getProcedure(Class<P> procClass) {
    return (P) (this.class_procedures.get(procClass));
  }

  public final Histogram<TransactionType> getTransactionSuccessHistogram() {
    return (this.txnSuccess);
  }

  public final Histogram<TransactionType> getTransactionUnknownHistogram() {
    return (this.txnUnknown);
  }

  public final Histogram<TransactionType> getTransactionRetryHistogram() {
    return (this.txnRetry);
  }

  public final Histogram<TransactionType> getTransactionAbortHistogram() {
    return (this.txnAbort);
  }

  public final Histogram<TransactionType> getTransactionErrorHistogram() {
    return (this.txnErrors);
  }

  public final Histogram<TransactionType> getTransactionRetryDifferentHistogram() {
    return (this.txtRetryDifferent);
  }

  @Override
  public final void run() {
    Thread t = Thread.currentThread();
    t.setName(this.toString());

    resultStats = new ResultStats(this.transactionTypes);

    // Invoke setup session
    try (Connection conn = benchmark.makeConnection()) {
      this.setupSession(conn);
    } catch (Throwable ex) {
      throw new RuntimeException("Unexpected error when setting up the session " + this, ex);
    } finally {
      benchmark.returnConnection();
    }

    // Invoke initialize callback
    try {
      this.initialize();
    } catch (Throwable ex) {
      throw new RuntimeException("Unexpected error when initializing " + this, ex);
    }

    // wait for start
    workloadState.blockForStart();

    while (true) {

      // PART 1: Init and check if done

      State preState = workloadState.getGlobalState();

      // Do nothing
      if (preState == State.DONE) {
        if (!seenDone) {
          // This is the first time we have observed that the
          // test is done notify the global test state, then
          // continue applying load
          seenDone = true;
          workloadState.signalDone();
          break;
        }
      }

      // PART 2: Wait for work

      // Sleep if there's nothing to do.
      workloadState.stayAwake();

      Phase prePhase = workloadState.getCurrentPhase();
      if (prePhase == null) {
        continue;
      }

      // Grab some work and update the state, in case it changed while we
      // waited.

      SubmittedProcedure pieceOfWork = workloadState.fetchWork();

      prePhase = workloadState.getCurrentPhase();
      if (prePhase == null) {
        continue;
      }

      preState = workloadState.getGlobalState();

      switch (preState) {
        case DONE, EXIT, LATENCY_COMPLETE -> {
          // Once a latency run is complete, we wait until the next
          // phase or until DONE.
          LOG.warn("preState is {}? will continue...", preState);
          continue;
        }
        default -> {}
          // Do nothing
      }

      // PART 3: Execute work

      TransactionType transactionType =
          getTransactionType(pieceOfWork, prePhase, preState, workloadState);

      if (!transactionType.equals(TransactionType.INVALID)) {

        // TODO: Measuring latency when not rate limited is ... a little
        // weird because if you add more simultaneous clients, you will
        // increase latency (queue delay) but we do this anyway since it is
        // useful sometimes

        // Wait before transaction if specified
        long preExecutionWaitInMillis = getPreExecutionWaitInMillis(transactionType);

        if (preExecutionWaitInMillis > 0) {
          try {
            LOG.debug(
                "{} will sleep for {} ms before executing",
                transactionType.getName(),
                preExecutionWaitInMillis);

            Thread.sleep(preExecutionWaitInMillis);
          } catch (InterruptedException e) {
            LOG.error("Pre-execution sleep interrupted", e);
          }
        }

        long start = System.nanoTime();

        TransactionStatus status = doWork(configuration.getDatabaseType(), transactionType);

        long end = System.nanoTime();

        // PART 4: Record results

        State postState = workloadState.getGlobalState();

        switch (postState) {
          case MEASURE:
            // Non-serial measurement. Only measure if the state both
            // before and after was MEASURE, and the phase hasn't
            // changed, otherwise we're recording results for a query
            // that either started during the warmup phase or ended
            // after the timer went off.
            Phase postPhase = workloadState.getCurrentPhase();

            if (postPhase == null) {
              // Need a null check on postPhase since current phase being null is used in
              // WorkloadState
              // and ThreadBench as the indication that the benchmark is over. However, there's a
              // race
              // condition with postState not being changed from MEASURE to DONE yet, so we entered
              // the
              // switch. In this scenario, just break from the switch.
              break;
            }
            if (preState == State.MEASURE && postPhase.getId() == prePhase.getId()) {
              boolean success =
                  status == TransactionStatus.SUCCESS || status == TransactionStatus.USER_ABORTED;
              resultStats.addLatency(transactionType.getId(), start, end, success);
              intervalRequests.incrementAndGet();
            }
            if (prePhase.isLatencyRun()) {
              workloadState.startColdQuery();
            }
            break;
          case COLD_QUERY:
            // No recording for cold runs, but next time we will since
            // it'll be a hot run.
            if (preState == State.COLD_QUERY) {
              workloadState.startHotQuery();
            }
            break;
          default:
            // Do nothing
        }

        // wait after transaction if specified
        long postExecutionWaitInMillis = getPostExecutionWaitInMillis(transactionType);

        if (postExecutionWaitInMillis > 0) {
          try {
            LOG.debug(
                "{} will sleep for {} ms after executing",
                transactionType.getName(),
                postExecutionWaitInMillis);

            Thread.sleep(postExecutionWaitInMillis);
          } catch (InterruptedException e) {
            LOG.error("Post-execution sleep interrupted", e);
          }
        }
      }

      workloadState.finishedWork();
    }

    LOG.debug("worker calling teardown");

    tearDown();
  }

  private TransactionType getTransactionType(
      SubmittedProcedure pieceOfWork, Phase phase, State state, WorkloadState workloadState) {
    TransactionType type = TransactionType.INVALID;

    try {
      type = transactionTypes.getType(pieceOfWork.getType());
    } catch (IndexOutOfBoundsException e) {
      if (phase.isThroughputRun()) {
        LOG.error("Thread tried executing disabled phase!");
        throw e;
      }
      if (phase.getId() == workloadState.getCurrentPhase().getId()) {
        switch (state) {
          case WARMUP -> {
            // Don't quit yet: we haven't even begun!
            LOG.info("[Serial] Resetting serial for phase.");
            phase.resetSerial();
          }
          case COLD_QUERY, MEASURE -> {
            // The serial phase is over. Finish the run early.
            LOG.info("[Serial] Updating workload state to {}.", State.LATENCY_COMPLETE);
            workloadState.signalLatencyComplete();
          }
          default -> throw e;
        }
      }
    }

    return type;
  }

  protected final TransactionStatus doWorkStep(
      Connection conn,
      DatabaseType databaseType,
      TransactionType transactionType,
      int retryCount,
      int maxRetryCount) {

    TransactionStatus status;

    try {

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("%s %s attempting...", this, transactionType));
      }

      status = this.executeWork(conn, transactionType);

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            String.format(
                "%s %s completed with status [%s]...", this, transactionType, status.name()));
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("%s %s committing...", this, transactionType));
      }
      conn.commit();

    } catch (UserAbortException ex) {

      try {
        conn.rollback();
      } catch (SQLException ex2) {
        LOG.warn("SQLException caught while rolling back transaction.", ex2);
      }

      ABORT_LOG.debug(String.format("%s Aborted", transactionType), ex);
      status = TransactionStatus.USER_ABORTED;

    } catch (SQLException ex) {

      try {
        conn.rollback();
      } catch (SQLException ex2) {
        LOG.warn("SQLException caught while attempting to rollback transaction.", ex2);
      }

      if (isRetryable(ex)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              String.format(
                  "Retryable SQLException occurred during [%s]... current retry attempt [%d], max retry attempts [%d], sql state [%s], error code [%d].",
                  transactionType, retryCount, maxRetryCount, ex.getSQLState(), ex.getErrorCode()),
              ex);
        }
        status = TransactionStatus.RETRY;
      } else {
        LOG.warn(
            String.format(
                "SQLException occurred during [%s] and will not be retried... sql state [%s], error code [%d].",
                transactionType, ex.getSQLState(), ex.getErrorCode()),
            ex);
        status = TransactionStatus.ERROR;
      }
    }

    return status;
  }

  /**
   * Called in a loop in the thread to exercise the system under test. Each implementing worker
   * should return the TransactionType handle that was executed.
   *
   * @param databaseType TODO
   * @param transactionType TODO
   */
  protected final TransactionStatus doWork(
      DatabaseType databaseType, TransactionType transactionType) {

    TransactionStatus status = TransactionStatus.UNKNOWN;

    int retryCount = 0;
    int maxRetryCount = configuration.getMaxRetries();

    while (retryCount < maxRetryCount && workloadState.getGlobalState() != State.DONE) {

      try (Connection conn = benchmark.makeConnection()) {
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(configuration.getIsolationMode());

        status = doWorkStep(conn, databaseType, transactionType, retryCount, maxRetryCount);

      } catch (SQLException ex) {
        LOG.debug("{} failed to get a connection...", this, ex);
        status = TransactionStatus.RETRY_DIFFERENT;

      } finally {
        benchmark.returnConnection();
      }

      switch (status) {
        case UNKNOWN -> this.txnUnknown.put(transactionType);
        case SUCCESS -> this.txnSuccess.put(transactionType);
        case USER_ABORTED -> this.txnAbort.put(transactionType);
        case RETRY -> this.txnRetry.put(transactionType);
        case RETRY_DIFFERENT -> this.txtRetryDifferent.put(transactionType);
        case ERROR -> this.txnErrors.put(transactionType);
      }

      switch (status) {
        case RETRY, RETRY_DIFFERENT -> {
          if (++retryCount < maxRetryCount) {
            try {
              Thread.sleep(ThreadLocalRandom.current().nextLong(50L, 300L));
            } catch (InterruptedException ix) {
            }
          }
        }
        default -> {
          return status;
        }
      }
    }

    return status;
  }

  private boolean isRetryable(SQLException ex) {

    String sqlState = ex.getSQLState();
    int errorCode = ex.getErrorCode();

    LOG.debug("sql state [{}] and error code [{}]", sqlState, errorCode);

    if (sqlState == null) {
      return false;
    }

    if (ex instanceof SQLRecoverableException) {
      return true;
    }

    // ------------------
    // SqlServer: "SELECT TOP 10 * FROM sys.messages"
    // ------------------
    if (errorCode == 12222 && sqlState.equals("S0051")) {
      // Lock request time out period exceeded.
      return true;
    } else if (errorCode == 0 && sqlState.equals("HY008")) {
      // The query has timed out.
      return true;
    }

    // ------------------
    // MYSQL:
    // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html
    // ------------------
    if (errorCode == 1213 && sqlState.equals("40001")) {
      // MySQL ER_LOCK_DEADLOCK
      return true;
    } else if (errorCode == 1205 && sqlState.equals("40001")) {
      // MySQL ER_LOCK_WAIT_TIMEOUT
      return true;
    }

    // ------------------
    // POSTGRES: https://www.postgresql.org/docs/current/errcodes-appendix.html
    // ------------------
    // Postgres serialization_failure
    return errorCode == 0 && sqlState.equals("40001");
  }

  /**
   * Optional callback that can be used to initialize the Worker right before the benchmark
   * execution begins
   */
  protected void initialize() {
    // The default is to do nothing
  }

  /**
   * Set up the session by running a set of statements before benchmark execution begins. The path
   * of the file where a set of statements defined should be added in &lt;sessionsetupfile&gt;
   * &lt;/sessionsetupfile&gt;
   */
  protected void setupSession(Connection conn) {
    try {
      String setupSessionFile = configuration.getSessionSetupFile();
      if (setupSessionFile == null || setupSessionFile.isEmpty()) {
        return;
      }

      String statements = new String(Files.readAllBytes(Paths.get(setupSessionFile)));
      if (statements.isEmpty()) {
        return;
      }

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(statements);
      }
      // conn.commit();
    } catch (SQLException | IOException ex) {
      throw new RuntimeException("Failed setting up session", ex);
    }
  }

  /**
   * Invoke a single transaction for the given TransactionType
   *
   * @param conn TODO
   * @param txnType TODO
   * @return TODO
   * @throws UserAbortException TODO
   * @throws SQLException TODO
   */
  protected abstract TransactionStatus executeWork(Connection conn, TransactionType txnType)
      throws UserAbortException, SQLException;

  /** Called at the end of the test to do any clean up that may be required. */
  public void tearDown() {}

  public void initializeState() {
    this.workloadState = this.configuration.getWorkloadState();
  }

  protected long getPreExecutionWaitInMillis(TransactionType type) {
    return 0;
  }

  protected long getPostExecutionWaitInMillis(TransactionType type) {
    return 0;
  }
}
