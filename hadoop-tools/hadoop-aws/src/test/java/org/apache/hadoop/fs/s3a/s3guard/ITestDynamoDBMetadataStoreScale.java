/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AWSServiceThrottledException;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.scale.AbstractITestS3AMetadataStoreScale;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.s3guard.MetadataStoreTestBase.basicFileStatus;
import static org.junit.Assume.*;

/**
 * Scale test for DynamoDBMetadataStore.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestDynamoDBMetadataStoreScale
    extends AbstractITestS3AMetadataStoreScale {

  private static final Logger LOG = LoggerFactory.getLogger(
      ITestDynamoDBMetadataStoreScale.class);

  private static final long BATCH_SIZE = 25;

  private static final long SMALL_IO_UNITS = BATCH_SIZE / 4;

  private DynamoDBMetadataStore ddbms;

  private DynamoDB ddb;

  private Table table;

  private String tableName;

  /** was the provisioning changed in test_001_limitCapacity()? */
  private boolean isProvisionedChanged;

  private ProvisionedThroughputDescription originalCapacity;

  private int threads = 100;

  private final int operationsPerThread = 200;

  /**
   * Create the metadata store. The table and region are determined from
   * the attributes of the FS used in the tests.
   * @return a new metadata store instance
   * @throws IOException failure to instantiate
   * @throws AssumptionViolatedException if the FS isn't running S3Guard + DDB/
   */
  @Override
  public MetadataStore createMetadataStore() throws IOException {
    S3AFileSystem fs = getFileSystem();
    assumeTrue("S3Guard is disabled for " + fs.getUri(),
        fs.hasMetadataStore());
    MetadataStore store = fs.getMetadataStore();
    assumeTrue("Metadata store for " + fs.getUri() + " is " + store
            + " -not DynamoDBMetadataStore",
        store instanceof DynamoDBMetadataStore);

    DynamoDBMetadataStore fsStore = (DynamoDBMetadataStore) store;
    Configuration conf = new Configuration(fs.getConf());

    tableName = fsStore.getTableName();
    assertTrue("Null/Empty tablename in " + fsStore,
        StringUtils.isNotEmpty(tableName));
    conf.set(S3GUARD_DDB_TABLE_NAME_KEY, tableName);
    String region = fsStore.getRegion();
    assertTrue("Null/Empty region in " + fsStore,
        StringUtils.isNotEmpty(region));
    conf.set(S3GUARD_DDB_REGION_KEY, region);
    conf.set(S3GUARD_DDB_RETRY_INTERVAL, "10ms");
    conf.set(S3GUARD_DDB_MAX_RETRIES, "2");

    DynamoDBMetadataStore ms = new DynamoDBMetadataStore();
    ms.initialize(conf);
    return ms;
  }


  @Override
  public void setup() throws Exception {
    super.setup();
    ddbms = (DynamoDBMetadataStore) createMetadataStore();
    tableName = ddbms.getTableName();
    assertNotNull("table has no name", tableName);
    ddb = ddbms.getDynamoDB();
    table = ddb.getTable(tableName);
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.cleanupWithLogger(LOG, ddbms);
    super.teardown();
  }

  /**
   * Attempt to limit capacity before any of the test cases.
   * This can fail if the capacity change count has been exceeded.
   */
  @Test
  public void test_001_limitCapacity() throws Throwable {
    originalCapacity = table.describe().getProvisionedThroughput();

    // If you set the same provisioned I/O as already set it throws an
    // exception, avoid that.
    isProvisionedChanged = (
        originalCapacity.getReadCapacityUnits() > SMALL_IO_UNITS
            || originalCapacity.getWriteCapacityUnits() > SMALL_IO_UNITS);

    if (isProvisionedChanged) {
      // Set low provisioned I/O for dynamodb
      describe("Provisioning dynamo tbl %s read/write -> %d/%d", tableName,
          SMALL_IO_UNITS, SMALL_IO_UNITS);
      try {
        // Blocks to ensure table is back to ready state before we proceed
        ddbms.provisionTableBlocking(SMALL_IO_UNITS, SMALL_IO_UNITS);
      } catch (AWSServiceThrottledException e) {
        // there's a limit to how often you can change sizes.
        LOG.warn("Failed to set capacity: " + e, e);
        isProvisionedChanged = false;
        throw e;
      }
    } else {
      describe("Skipping provisioning table I/O, already %d/%d",
          SMALL_IO_UNITS, SMALL_IO_UNITS);
    }
  }


  /**
   * Though the AWS SDK claims in documentation to handle retries and
   * exponential backoff, we have witnessed
   * com.amazonaws...dynamodbv2.model.ProvisionedThroughputExceededException
   * (Status Code: 400; Error Code: ProvisionedThroughputExceededException)
   * Hypothesis:
   * Happens when the size of a batched write is bigger than the number of
   * provisioned write units.  This test ensures we handle the case
   * correctly, retrying w/ smaller batch instead of surfacing exceptions.
   */
  @Test
  public void test_030_BatchedWriteExceedsProvisioned() throws Exception {

    final long iterations = 5;
    List<PathMetadata> toCleanup = new ArrayList<>();

    // Fail if someone changes a constant we depend on
    assertTrue("Maximum batch size must big enough to run this test",
        S3GUARD_DDB_BATCH_WRITE_REQUEST_LIMIT >= BATCH_SIZE);

    ThrottleTracker throttles = new ThrottleTracker();

    // We know the dynamodb metadata store will expand a put of a path
    // of depth N into a batch of N writes (all ancestors are written
    // separately up to the root).  (Ab)use this for an easy way to write
    // a batch of stuff that is bigger than the provisioned write units
    try {
      describe("Running %d iterations of batched put, size %d", iterations,
          BATCH_SIZE);
      long pruneItems = 0;
      for (long i = 0; i < iterations; i++) {
        Path longPath = pathOfDepth(BATCH_SIZE, String.valueOf(i));
        FileStatus status = basicFileStatus(longPath, 0, false, 12345,
            12345);
        PathMetadata pm = new PathMetadata(status);

        ddbms.put(pm);
        toCleanup.add(pm);
        pruneItems++;

        // now do an aggressive series of lookups within each iteration
        for (int j = 0; j < 500; j++) {
          ddbms.get(longPath);
        }
        for (int j = 0; j < 20; j++) {
          ddbms.listChildren(longPath.getParent());
        }

        // Having hard time reproducing Exceeded exception with put, also
        // try occasional prune, which was the only stack trace I've seen
        // (on JIRA)
        if (pruneItems == BATCH_SIZE) {
          describe("pruning files");
          ddbms.prune(Long.MAX_VALUE /* all files */);
          pruneItems = 0;
        }
      }

      throttles.complete();

    } finally {
      describe("Cleaning up table %s", tableName);
      for (PathMetadata pm : toCleanup) {
        cleanupMetadata(ddbms, pm);
      }
    }
    LOG.info("Completed with {}", throttles);
    throttles.assertThrottlingDetected();
  }

  @Test
  public void test_040_getThrottling() throws Throwable {
    // attempt to create many many get requests in parallel.
    Path path = new Path("s3a://example.org/TODO");
    S3AFileStatus status = new S3AFileStatus(true, path, "alice");
    PathMetadata metadata = new PathMetadata(status);
    try {
      ddbms.put(metadata);
      execute("get",
          operationsPerThread,
          () -> ddbms.get(path),
          true);
    } finally {
      ddbms.delete(path);
    }
  }

  @Test
  public void test_050_listThrottling() throws Throwable {
    // attempt to create many many get requests in parallel.
    Path path = new Path("s3a://example.org/TODO");
    S3AFileStatus status = new S3AFileStatus(true, path, "alice");
    PathMetadata metadata = new PathMetadata(status);
    ddbms.put(metadata);
    try {
      Path parent = path.getParent();
      execute("list",
          operationsPerThread,
          () -> {
            ddbms.listChildren(parent);
          },
          true);
    } finally {
      ddbms.delete(path);
    }
  }

  /**
   * Execute a set of operations in parallel, collect throttling statistics
   * and return them.
   * @param operation string for messages.
   * @param operationsPerThread number of times per thread to invoke the action.
   * @param action action to invoke.
   * @param expectThrottling is throttling expected (and to be asserted on?)
   * @return the throttle statistics
   */
  public ThrottleTracker execute(String operation,
      int operationsPerThread,
      LambdaTestUtils.VoidCallable action, final boolean expectThrottling)
      throws Exception {

    final ContractTestUtils.NanoTimer timer = new ContractTestUtils.NanoTimer();
    final ThrottleTracker throttles = new ThrottleTracker();
    final ExecutorService executorService = Executors.newFixedThreadPool(threads);
    final List<Callable<Integer>> tasks = new ArrayList<>(threads);

    final AtomicInteger throttleExceptions = new AtomicInteger(0);
    for (int i = 0; i < threads; i++) {
      tasks.add(
          () -> {
            final ContractTestUtils.NanoTimer t
                = new ContractTestUtils.NanoTimer();
            int completed = 0;
            for (int j = 0; j < operationsPerThread; j++) {
              try {
                action.call();
                completed++;
              } catch (AWSServiceThrottledException e) {
                // this is good
                throttleExceptions.incrementAndGet();
                // consider it completed
                completed++;
              } catch (Exception e) {
                LOG.error("Failed to execute {}", operation, e);
                break;
              }
            }
            LOG.info("Thread completed {} with {} operations in {} millis",
                operation, completed, t.elapsedTimeMs());
            return completed;
          }
      );
    }
    List<Future<Integer>> futures = executorService.invokeAll(tasks,
        getTestTimeoutMillis(), TimeUnit.MILLISECONDS);
    long elapsedMs = timer.elapsedTimeMs();
    LOG.info("Completed {} with {}", operation, throttles);
    LOG.info("time to execute: {} millis", elapsedMs);

    for (Future<Integer> future : futures) {
      assertTrue("Future timed out", future.isDone());
    }
    if (expectThrottling) {
      throttles.assertThrottlingDetected();
    }
    for (Future<Integer> future : futures) {
      assertEquals("Future did not complete all operations",
          operationsPerThread, future.get().intValue());
    }

    return throttles;
  }

  /**
   * Restore the capacity if it was set in {@link #test_001_limitCapacity}.
   */
  @Test
  public void test_999_restore_capacity() throws Throwable {
    assumeTrue("Provisioning didn't change", isProvisionedChanged);
    long write = originalCapacity.getWriteCapacityUnits();
    long read = originalCapacity.getReadCapacityUnits();
    describe("Restoring dynamo tbl %s read/write -> %d/%d", tableName,
        read, write);
    ddbms.provisionTableBlocking(read, write);
  }

  // Attempt do delete metadata, suppressing any errors
  private void cleanupMetadata(MetadataStore ms, PathMetadata pm) {
    try {
      ms.forgetMetadata(pm.getFileStatus().getPath());
    } catch (IOException ioe) {
      // Ignore.
    }
  }

  private Path pathOfDepth(long n, @Nullable String fileSuffix) {
    StringBuilder sb = new StringBuilder();
    for (long i = 0; i < n; i++) {
      sb.append(i == 0 ? "/" + this.getClass().getSimpleName() : "lvl");
      sb.append(i);
      if (i == n - 1 && fileSuffix != null) {
        sb.append(fileSuffix);
      }
      sb.append("/");
    }
    return new Path(getFileSystem().getUri().toString(), sb.toString());
  }

  /**
   * Something to track those throttles.
   */
  private class ThrottleTracker {

    private final long writeThrottleEventOrig = ddbms.getWriteThrottleEventCount();

    private final long readThrottleEventOrig = ddbms.getReadThrottleEventCount();

    private final long batchWriteThrottleCountOrig
        = ddbms.getBatchWriteCapacityExceededCount();

    private long readThrottles;

    private long writeThrottles;

    private long batchThrottles;

    void complete() {
      readThrottles = ddbms.getReadThrottleEventCount() - readThrottleEventOrig;
      writeThrottles = ddbms.getWriteThrottleEventCount()
          - writeThrottleEventOrig;
      batchThrottles = ddbms.getBatchWriteCapacityExceededCount()
          - batchWriteThrottleCountOrig;
    }

    @Override
    public String toString() {
      return String.format(
          "Read throttle events = %d; write events = %d; batch throttles = %d",
          readThrottles, writeThrottles, batchThrottles);
    }

    public void assertThrottlingDetected() {
      assertTrue("No throttling detected",
          readThrottles > 0 || writeThrottles > 0 || batchThrottles > 0);
    }
  }
}
