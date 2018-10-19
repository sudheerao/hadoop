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

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.IOException;
import java.net.URI;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is the base class for both the delegation binding
 * code and the back end service created; allows for
 * shared methods across both.
 */
public abstract class AbstractDTService
    extends AbstractService {

  /**
   * URI of the filesystem.
   * Valid after {@link #bindToFileSystem(URI, S3AFileSystem)}
   * or {@link #bindToURI(URI)}.
   */
  private URI canonicalUri;

  /**
   * The owning filesystem.
   * Valid after {@link #bindToFileSystem(URI, S3AFileSystem)}.
   */
  private S3AFileSystem fileSystem;

  /**
   * Owner of the filesystem.
   * Valid after {@link #bindToFileSystem(URI, S3AFileSystem)}.
   */
  private UserGroupInformation owner;

  /**
   * Protected constructor.
   * @param name service name.
   */
  protected AbstractDTService(final String name) {
    super(name);
  }

  /**
   * Bind to the filesystem.
   * Subclasses can use this to perform their own binding operations -
   * but they must always call their superclass implementation.
   * This <i>Must</i> be called before calling {@code init()}.
   * @param uri the canonical URI of the FS.
   * @param fs owning FS.
   * @throws IOException failure.
   */
  public void bindToFileSystem(
      final URI uri,
      final S3AFileSystem fs) throws IOException {
    requireServiceState(STATE.NOTINITED);
    Preconditions.checkState(canonicalUri == null,
        "bindToFileSystem called twice");
    this.canonicalUri = checkNotNull(uri);
    this.fileSystem = checkNotNull(fs);
    this.owner = fs.getOwner();
  }

  /**
   * Get the canonical URI of the filesystem, which is what is
   * used to identify the tokens.
   * @return the URI.
   */
  public URI getCanonicalUri() {
    return canonicalUri;
  }

  /**
   * Get the owner of the FS.
   * Will be null if the service was initialized though
   * {@link #bindToURI(URI)} rather than
   * {@link #bindToFileSystem(URI, S3AFileSystem)}
   * @return the owner fs; null if a filesystem wasn't supplied during binding.
   */
  protected S3AFileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * Get the owner of this Service.
   * @return owner; non-null after binding to an FS.
   */
  public UserGroupInformation getOwner() {
    return owner;
  }

  /**
   * Require that the service is in a given state.
   * @param state desired state.
   * @throws IllegalStateException if the condition is not met
   */
  protected void requireServiceState(final STATE state)
      throws IllegalStateException {
    Preconditions.checkState(isInState(state),
        "Required State: %s; Actual State %s", state, getServiceState());
  }

  /**
   * Require the service to be started.
   * @throws IllegalStateException if it is not.
   */
  protected void requireServiceStarted() throws IllegalStateException {
    requireServiceState(STATE.STARTED);
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    super.serviceInit(conf);
    checkNotNull(canonicalUri, "service does not have a canonical URI");
  }
}
