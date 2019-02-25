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

import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE_CLIENT;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE_NONE;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE_SERVER;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_MODE_WARN;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_SOURCE;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_SOURCE_DEFAULT;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_SOURCE_ETAG;
import static org.apache.hadoop.fs.s3a.Constants.CHANGE_DETECT_SOURCE_VERSION_ID;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.util.Locale;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object change detection policy.  Determines which attribute is used to detect change and what to do when change is
 * detected.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class S3AChangeDetectionPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3AChangeDetectionPolicy.class);

  private final Mode mode;

  /**
   * The S3 object attribute used to detect change.
   */
  public enum Source {
    eTag(CHANGE_DETECT_SOURCE_ETAG),
    versionId(CHANGE_DETECT_SOURCE_VERSION_ID);

    private final String source;

    Source(String source) {
      this.source = source;
    }

    private static Source fromString(String trimmed) {
      for (Source value : values()) {
        if (value.source.equals(trimmed)) {
          return value;
        }
      }
      LOG.warn("Unrecognized " + CHANGE_DETECT_SOURCE + " value: \"{}\"", trimmed);
      return fromString(CHANGE_DETECT_SOURCE_DEFAULT);
    }

    static Source fromConfiguration(Configuration configuration) {
      String trimmed = configuration.get(CHANGE_DETECT_SOURCE, CHANGE_DETECT_SOURCE_DEFAULT).trim()
          .toLowerCase(Locale.ENGLISH);
      return fromString(trimmed);
    }
  }

  /**
   * What to do when change is detected.
   */
  public enum Mode {
    Client(CHANGE_DETECT_MODE_CLIENT), Server(CHANGE_DETECT_MODE_SERVER), Warn(
        CHANGE_DETECT_MODE_WARN), None(CHANGE_DETECT_MODE_NONE);

    private final String mode;

    Mode(String mode) {
      this.mode = mode;
    }

    private static Mode fromString(String trimmed) {
      for (Mode value : values()) {
        if (value.mode.equals(trimmed)) {
          return value;
        }
      }
      LOG.warn("Unrecognized " + CHANGE_DETECT_MODE + " value: \"{}\"", trimmed);
      return fromString(CHANGE_DETECT_MODE_DEFAULT);
    }

    static Mode fromConfiguration(Configuration configuration) {
      String trimmed = configuration.get(CHANGE_DETECT_MODE, CHANGE_DETECT_MODE_DEFAULT).trim()
          .toLowerCase(Locale.ENGLISH);
      return fromString(trimmed);
    }
  }

  S3AChangeDetectionPolicy(Mode mode) {
    this.mode = mode;
  }

  public Mode getMode() {
    return mode;
  }

  public abstract Source getSource();

  /**
   * Reads the change detection policy from Configuration.  Returns a policy even if the Configuration is bogus.
   *
   * @param configuration the configuration
   * @return the policy
   */
  public static S3AChangeDetectionPolicy getPolicy(Configuration configuration) {
    Mode mode = Mode.fromConfiguration(configuration);
    switch (Source.fromConfiguration(configuration)) {
      case eTag:
        return new ETagChangeDetectionPolicy(mode);
      case versionId:
        return new VersionIdChangeDetectionPolicy(mode);
    }
    throw new IllegalStateException("Unsupported change detection mode:" + mode);
  }

  /**
   * Pulls the attribute this policy uses to detect change out of the S3 object metadata.  The policy calls generically
   * refers to this attribute as {@code revisionId}.
   *
   * @param objectMetadata the s3 object metadata
   * @param uri the URI of the object
   * @return the revisionId string as interpreted by this policy, or potentially null if the attribute is unavailable
   * (such as when the policy says to use versionId but object versioning is not enabled for the bucket).
   */
  public abstract String getRevisionId(ObjectMetadata objectMetadata, String uri);

  /**
   * Applies the given {@link #getRevisionId(ObjectMetadata, String) revisionId} as a server-side qualification on the
   * {@code GetObjectRequest}.
   *
   * @param request the request
   * @param revisionId the revision id
   */
  public abstract void applyRevisionConstraint(GetObjectRequest request, String revisionId);

  /**
   * Takes appropriate action based on {@link #getMode() mode} when a change has been detected.
   *
   * @param revisionId the expected revision id
   * @param newRevisionId the detected revision id
   * @param uri the URI of the object being accessed
   * @param position the position being read in the object
   * @param operation the operation being performed on the object (e.g. open or re-open) that triggered the change
   * detection
   * @throws RemoteFileChangedException if the configured mode treats the condition as an exception
   */
  public void onChangeDetected(String revisionId, String newRevisionId, String uri, long position, String operation)
      throws RemoteFileChangedException {
    switch (mode) {
      case None:
        // really this shouldn't even be called when mode == None
        break;
      case Warn:
        LOG.warn(String.format("%s change detected on %s %s at %d. Expected %s got %s",
            getSource(), operation, uri, position, revisionId, newRevisionId));
        break;
      default:
        // mode == Client (or Server, but really won't be called for Server)
        throw new RemoteFileChangedException(uri, operation,
            String.format("%s change detected while reading at position %s. Expected %s got %s",
                getSource(), position, revisionId, newRevisionId));
    }
  }

  /**
   * Change detection policy based on {@link ObjectMetadata#getETag() eTag}.
   */
  static class ETagChangeDetectionPolicy extends S3AChangeDetectionPolicy {

    ETagChangeDetectionPolicy(Mode mode) {
      super(mode);
    }

    @Override
    public String getRevisionId(ObjectMetadata objectMetadata, String uri) {
      return objectMetadata.getETag();
    }

    @Override
    public void applyRevisionConstraint(GetObjectRequest request, String revisionId) {
      request.withMatchingETagConstraint(revisionId);
    }

    @Override
    public Source getSource() {
      return Source.eTag;
    }
  }

  /**
   * Change detection policy based on {@link ObjectMetadata#getVersionId() versionId}.
   */
  static class VersionIdChangeDetectionPolicy extends S3AChangeDetectionPolicy {

    public VersionIdChangeDetectionPolicy(Mode mode) {
      super(mode);
    }

    @Override
    public String getRevisionId(ObjectMetadata objectMetadata, String uri) {
      String versionId = objectMetadata.getVersionId();
      if (versionId == null) {
        // this policy doesn't work if the bucket doesn't have object versioning enabled (which isn't by default)
        LOG.warn(CHANGE_DETECT_MODE + " set to " + Source.versionId
                + " but no versionId available while reading {}. Ensure your bucket has object versioning enabled. "
                + "You may see inconsistent reads.",
            uri);
      }
      return versionId;
    }

    @Override
    public void applyRevisionConstraint(GetObjectRequest request, String revisionId) {
      request.withVersionId(revisionId);
    }

    public Source getSource() {
      return Source.versionId;
    }
  }
}
