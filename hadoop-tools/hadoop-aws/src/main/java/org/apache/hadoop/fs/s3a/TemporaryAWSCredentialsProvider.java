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

import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;

import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.auth.AbstractSessionCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;

/**
 * Support session credentials for authenticating with AWS.
 *
 * Please note that users may reference this class name from configuration
 * property fs.s3a.aws.credentials.provider.  Therefore, changing the class name
 * would be a backward-incompatible change.
 *
 * This credential provider must not fail in creation because that will
 * break a chain of credential providers.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TemporaryAWSCredentialsProvider extends
    AbstractSessionCredentialsProvider {

  public static final String NAME
      = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";

  public TemporaryAWSCredentialsProvider(Configuration conf)
      throws IOException {
    this(null, conf);
  }

  public TemporaryAWSCredentialsProvider(URI uri, Configuration conf)
      throws IOException {
    super(uri, conf);
    // no attempt is made to call init() here as this provider isn't
    // expected to fail-on-instantiate.
    // instead the on-demand call in createCredentials will trigger the lookup
    // and any failures.
  }

  /**
   * The credentials here must include a session token, else this operation
   * will raise an exception.
   * @param config the configuration
   * @return temporary credentials.
   * @throws IOException on any failure to load the credentials.
   */
  @Override
  protected AWSCredentials createCredentials(Configuration config)
      throws IOException {
    return MarshalledCredentials.load(getUri(), config)
        .toAWSCredentials(
            MarshalledCredentials.CredentialTypeRequired.SessionOnly);
  }

}
