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

import com.amazonaws.auth.AWSCredentials;
import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.CredentialInitializationException;
import org.apache.hadoop.fs.s3a.auth.AbstractSessionCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.SessionCredentials;

/**
 * AWS credential provider driven from marshalled session/full credentials
 * (full, simple session or role).
 * This is <i>not</i> intended for explicit use in job/app configurations,
 * instead it is returned by Delegation Token Bindings, as needed.
 * The constructor implicitly prevents explicit use.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MarshalledCredentialProvider extends
    AbstractSessionCredentialsProvider {

  /** Name: {@value}. */
  public static final String NAME
      = "org.apache.hadoop.fs.s3a.auth.delegation.MarshalledCredentialProvider";

  private final SessionCredentials credentials;
  private final boolean sessionTokenRequired;

  /**
   * Constructor.
   * @param uri filesystem URI
   * @param conf configuration.
   * @param credentials marshalled credentials.
   * @param sessionTokenRequired flag to indicate that the marshalled
   * credentials must include a session token.
   * @throws CredentialInitializationException validation failure
   * @throws IOException failure
   */
  public MarshalledCredentialProvider(
      final URI uri,
      final Configuration conf,
      final SessionCredentials credentials,
      final boolean sessionTokenRequired)
      throws IOException {
    super(uri, conf);
    Preconditions.checkArgument(uri != null, "No filesystem URI");
    this.sessionTokenRequired = sessionTokenRequired;
    this.credentials = credentials;
    init();
  }

  /**
   * Perform the binding, looking up the DT and parsing it.
   * @return true if there were some credentials
   * @throws CredentialInitializationException validation failure
   * @throws IOException on a failure
   */
  @Override
  protected AWSCredentials createCredentials(final Configuration config)
      throws IOException {
    return credentials.toAWSCredentials(sessionTokenRequired);
  }

}
