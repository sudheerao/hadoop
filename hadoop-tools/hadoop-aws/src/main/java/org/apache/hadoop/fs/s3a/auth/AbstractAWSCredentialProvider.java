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

package org.apache.hadoop.fs.s3a.auth;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Optional;

import com.amazonaws.auth.AWSCredentialsProvider;

import org.apache.hadoop.conf.Configuration;

/**
 * Base class for AWS credential providers which
 * take an optional URI and config in their constructor.
 * 
 * The call
 * {@link org.apache.hadoop.fs.s3a.S3AUtils#createAWSCredentialProvider(Configuration, Class, Optional)}
 * looks for the {@code Optional<URI>} constructor first.
 */
public abstract class AbstractAWSCredentialProvider 
    implements AWSCredentialsProvider {

  private final Optional<URI> binding;

  private final Configuration conf;

  /**
   * Constructor.
   * @param binding optional filesystem URI.
   * @param conf configuration.
   */
  protected AbstractAWSCredentialProvider(
      final Optional<URI> binding,
      final Configuration conf) {
    this.binding = binding;
    this.conf = conf;
  }

  /**
   * Construct from URI + configuration.
   * @param uri URI: may be null.
   * @param conf configuration.
   */
  protected AbstractAWSCredentialProvider(
      @Nullable final URI uri,
      final Configuration conf) {
    this.conf = conf;
    this.binding = Optional.ofNullable(uri);
  }

  public Optional<URI> getBinding() {
    return binding;
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Get the binding URI: may be null.
   * @return the URI this instance was constructed with,
   * if any.
   */
  public URI getUri() {
    return binding.orElse(null);
  }

  /**
   * Refresh is a no-op by default.
   */
  @Override
  public void refresh() {
  }
}
