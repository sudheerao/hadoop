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

package org.apache.hadoop.fs.s3a;

import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.s3a.impl.ChangeTracker;
import org.apache.hadoop.test.HadoopTestBase;

import static org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.CHANGE_DETECTED;
import static org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.createPolicy;
import static org.apache.hadoop.fs.s3a.impl.ChangeTracker.CHANGE_REPORTED_BY_S3;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test {@link ChangeTracker}
 */
public class TestStreamChangeTracker extends HadoopTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStreamChangeTracker.class);

  public static final String BUCKET = "bucket";

  public static final String OBJECT = "object";

  public static final String URI = "s3a://" + BUCKET + "/" + OBJECT;

  @Test
  public void testVersionCheckingHandlingNoVersions() throws Throwable {
    LOG.info("If an endpoint doesn't return versions, that's OK");
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Client,
        ChangeDetectionPolicy.Source.VersionId);
    assertFalse("Tracker should not have applied contraints " + tracker,
        tracker.maybeApplyConstraint(newGetObjectRequest()));
    tracker.processResponse(
        newResponse(null, null),
        "", 0);
    assertTrackerMismatchCount(tracker, 0);
  }

  @Test
  public void testEtagCheckingWarn() throws Throwable {
    LOG.info("If an endpoint doesn't return errors, that's OK");
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Warn,
        ChangeDetectionPolicy.Source.ETag);
    assertFalse("Tracker should not have applied contraints " + tracker,
        tracker.maybeApplyConstraint(newGetObjectRequest()));
    tracker.processResponse(
        newResponse("e1", null),
        "", 0);
    tracker.processResponse(
        newResponse("e1", null),
        "", 0);
    tracker.processResponse(
        newResponse("e2", null),
        "", 0);
    assertTrackerMismatchCount(tracker, 1);
    // subsequent error does not trigger a second warning
    tracker.processResponse(
        newResponse("e2", null),
        "", 0);
    assertTrackerMismatchCount(tracker, 1);
  }

  @Test
  public void testVersionCheckingOnClient() throws Throwable {
    LOG.info("Verify the client-side version checker raises exceptions");
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Client,
        ChangeDetectionPolicy.Source.VersionId);
    assertFalse("Tracker should not have applied contraints " + tracker,
        tracker.maybeApplyConstraint(newGetObjectRequest()));
    tracker.processResponse(
        newResponse(null, "rev1"),
        "", 0);
    assertTrackerMismatchCount(tracker, 0);
    assertRevisionId(tracker, "rev1");
    GetObjectRequest request = newGetObjectRequest();
    assertEquals("Request version ID", "rev1", request.getVersionId());
    expectChangeException(tracker, newResponse(null, "rev2"), "change detected");
    // mismatch was noted (so gets to FS stats)
    assertTrackerMismatchCount(tracker, 1);
    // new revision was picked up (in case someone tries again)
    assertRevisionId(tracker, "rev2");
  }

  @Test
  public void testVersionCheckingOnServer() throws Throwable {
    LOG.info("Verify the client-side version checker handles null-ness");
    ChangeTracker tracker = newTracker(
        ChangeDetectionPolicy.Mode.Server,
        ChangeDetectionPolicy.Source.VersionId);
    assertFalse("Tracker should not have applied contraints " + tracker,
        tracker.maybeApplyConstraint(newGetObjectRequest()));
    tracker.processResponse(
        newResponse(null, "rev1"),
        "", 0);
    assertTrackerMismatchCount(tracker, 0);
    assertRevisionId(tracker, "rev1");
    GetObjectRequest request = newGetObjectRequest();
    assertConstraintApplied(tracker, request);
    // now, the tracker expects a null response
    expectChangeException(tracker, null, CHANGE_REPORTED_BY_S3);
    assertTrackerMismatchCount(tracker, 1);

    // now, imagine the server doesn't trigger a failure due to some
    // bug in its logic
    // we should still react to the reported value
    expectChangeException(tracker,
        newResponse(null, "rev2"),
        CHANGE_DETECTED);
  }

  protected void assertConstraintApplied(final ChangeTracker tracker,
      final GetObjectRequest request) {
    assertTrue("Tracker should have applied contraints " + tracker,
        tracker.maybeApplyConstraint(request));
  }
  
  protected RemoteFileChangedException expectChangeException(
      final ChangeTracker tracker,
      final S3Object response, 
      final String message) throws Exception {
    return intercept(
        RemoteFileChangedException.class,
        message,
        () -> {
          tracker.processResponse(response, "", 0);
          return tracker;
        });
  }

  protected void assertRevisionId(final ChangeTracker tracker,
      final String revId) {
    assertEquals("Wrong revision ID in " + tracker,
        revId, tracker.getRevisionId());
  }


  protected void assertTrackerMismatchCount(
      final ChangeTracker tracker,
      final int expectedCount) {
    assertEquals("counter in tracker " + tracker,
        expectedCount, tracker.getVersionMismatches().get());
  }

  /**
   * Create tracker. 
   * Contains standard assertions(s).
   * @return the tracker.
   */
  protected ChangeTracker newTracker(final ChangeDetectionPolicy.Mode mode,
      final ChangeDetectionPolicy.Source source) {
    ChangeDetectionPolicy policy = createPolicy(
        mode,
        source);
    ChangeTracker tracker = new ChangeTracker(URI, policy,
        new AtomicLong(0));
    assertFalse("Tracker should not have applied contraints " + tracker,
        tracker.maybeApplyConstraint(newGetObjectRequest()));
    return tracker;
  }

  private GetObjectRequest newGetObjectRequest() {
    return new GetObjectRequest(BUCKET, OBJECT);
  }
  
  private S3Object newResponse(String etag, String revision) {
    ObjectMetadata md = new ObjectMetadata();
    if (etag != null) {
      md.setHeader(Headers.ETAG, etag);
    }
    if (revision != null) {
      md.setHeader(Headers.S3_VERSION_ID, revision);
    }
    S3Object response = emptyResponse();
    response.setObjectMetadata(md);
    return response;    
  }

  private S3Object emptyResponse() {
    S3Object response = new S3Object();
    response.setBucketName(BUCKET);
    response.setKey(OBJECT);
    return response;
  }
}
