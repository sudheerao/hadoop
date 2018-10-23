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
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentialProvider;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;

import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.FULL_TOKEN_KIND;

/**
 * Full credentials: they are simply passed as-is, rather than
 * converted to a session.
 * These aren't as secure; this class exists to (a) support deployments
 * where there is not STS service and (b) validate the design of
 * S3A DT support to support different managers.
 *
 */
public class FullCredentialsTokenBinding extends
    AbstractDelegationTokenBinding {

  /**
   * Wire name of this binding includes a version marker: {@value}.
   */
  private static final String NAME = "FullCredentials-001";

  /**
   * Constructor, uses name of {@link #name} and token kind of
   * {@link DelegationConstants#FULL_TOKEN_KIND}.
   *
   */
  public FullCredentialsTokenBinding() {
    super(NAME, FULL_TOKEN_KIND);
  }

  /**
   * Create the DT.
   *
   * It's slightly inefficient to create a new one every time, but
   * it avoids concurrency problems with managing any singleton.
   * @param policy minimum policy to use, if known.
   * @param encryptionSecrets encryption secrets.
   * @return a DT identifier
   * @throws IOException failure
   */
  @Override
  public AbstractS3ATokenIdentifier createTokenIdentifier(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets) throws IOException {
    requireServiceStarted();
    Configuration conf = getConfig();
    URI uri = getCanonicalUri();
    String origin = AbstractS3ATokenIdentifier.createDefaultOriginMessage();
    MarshalledCredentials marshalledCredentials;
    // look for access keys to FS
    S3xLoginHelper.Login secrets = S3AUtils.getAWSAccessKeys(uri, conf);
    if (secrets.hasLogin()) {
      marshalledCredentials = new MarshalledCredentials(
          secrets.getUser(), secrets.getPassword(), "");
      origin += "; source = Hadoop configuration data";
    } else {
      // if there are none, look for the environment variables.
      marshalledCredentials = MarshalledCredentials.fromEnvironment(System.getenv());
      origin += "; source = Environment variables";
    }
    marshalledCredentials.validate("local AWS credentials", false);

    final FullCredentialsTokenIdentifier id
        = new FullCredentialsTokenIdentifier(getCanonicalUri(),
        getOwnerText(),
        marshalledCredentials,
        encryptionSecrets);
    id.setOrigin(origin);
    return id;
  }

  @Override
  public AWSCredentialProviderList bindToTokenIdentifier(
      final AbstractS3ATokenIdentifier retrievedIdentifier)
      throws IOException {
    FullCredentialsTokenIdentifier tokenIdentifier =
        convertTokenIdentifier(retrievedIdentifier,
            FullCredentialsTokenIdentifier.class);
    return new AWSCredentialProviderList(
        new MarshalledCredentialProvider(
            getFileSystem().getUri(),
            getConfig(),
            tokenIdentifier.getMarshalledCredentials(),
            false));
  }

  @Override
  public AbstractS3ATokenIdentifier createIdentifier() {
    return new FullCredentialsTokenIdentifier();
  }

}
