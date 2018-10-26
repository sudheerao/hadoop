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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3ARetryPolicy;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentialProvider;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.fs.s3a.auth.RoleModel;
import org.apache.hadoop.fs.s3a.auth.STSClientFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Invoker.once;
import static org.apache.hadoop.fs.s3a.S3AUtils.createAWSCredentialProvider;
import static org.apache.hadoop.fs.s3a.S3AUtils.loadAWSProviderClasses;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.*;

/**
 * The session token DT binding: creates an AWS session token
 * for the DT, extracts and serves it up afterwards.
 */
public class SessionTokenBinding extends AbstractDelegationTokenBinding {

  private static final Logger LOG = LoggerFactory.getLogger(
      SessionTokenBinding.class);

  /**
   * Wire name of this binding: {@value}.
   */
  private static final String NAME = "SessionTokens/001";

  /**
   * A message added to the standard origin string when the DT is
   * built from session credentials passed in.
   */
  @VisibleForTesting
  public static final String CREDENTIALS_CONVERTED_TO_DELEGATION_TOKEN
      = "Existing session credentials converted to Delegation Token";

  /** Invoker for STS calls. */
  private Invoker invoker;

  /**
   * Has an attempt to initialize STS been attempted?
   */
  private boolean stsInitAttempted;

  /** The STS client; created in startup if the parental credentials permit. */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private Optional<STSClientFactory.STSClient> stsClient = Optional.empty();

  /**
   * duration of session.
   */
  private long duration;

  /**
   * Flag to indicate that the auth chain provides session credentials.
   * If true it means that STS cannot be used (and stsClient is null).
   */
  private boolean hasSessionCreds;

  /**
   * The auth chain for the parent options.
   */
  private AWSCredentialProviderList parentAuthChain;

  /**
   * Has a log message about forwarding credentials been printed yet?
   */
  private final AtomicBoolean forwardMessageLogged = new AtomicBoolean(false);

  /** Constructor for reflection. */
  public SessionTokenBinding() {
    this(NAME, SESSION_TOKEN_KIND);
  }

  /**
   * Constructor for subclasses.
   * @param name binding name.
   * @param kind token kind.
   */
  protected SessionTokenBinding(final String name,
      final Text kind) {
    super(name, kind);
  }

  /**
   * Service start will read in all configuration options
   * then build that client.
   */
  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    Configuration conf = getConfig();
    duration = conf.getTimeDuration(DELEGATION_TOKEN_DURATION,
        DEFAULT_DELEGATION_TOKEN_DURATION,
        TimeUnit.SECONDS);

    // create the provider set for session credentials.
    Class<?>[] awsClasses = loadAWSProviderClasses(conf,
        ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        SimpleAWSCredentialsProvider.class,
        EnvironmentVariableCredentialsProvider.class);

    parentAuthChain = new AWSCredentialProviderList();
    URI uri = getCanonicalUri();
    for (Class<?> clazz : awsClasses) {
      parentAuthChain.add(createAWSCredentialProvider(conf, clazz, uri));
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    this.stsClient.ifPresent(IOUtils::closeStream);
    this.stsClient = Optional.empty();
  }

  /**
   * Return an unbonded provider chain.
   * @return the auth chain built from the assumed role credentials
   * @throws IOException any failure.
   */
  @Override
  public AWSCredentialProviderList deployUnbonded()
      throws IOException {
    requireServiceStarted();
    return parentAuthChain;
  }

  /**
   * Get the invoker for STS calls.
   * @return the invoker
   */
  protected Invoker getInvoker() {
    return invoker;
  }

  @Override
  public AWSCredentialProviderList bindToTokenIdentifier(
      final AbstractS3ATokenIdentifier retrievedIdentifier)
      throws IOException {
    SessionTokenIdentifier tokenIdentifier =
        convertTokenIdentifier(retrievedIdentifier,
            SessionTokenIdentifier.class);
    return new AWSCredentialProviderList(
        new MarshalledCredentialProvider(
            getFileSystem().getUri(),
            getConfig(),
            tokenIdentifier.getMarshalledCredentials(),
            true));
  }

  /**
   * Attempt to init the STS connection, only does it once.
   * If the AWS credential list to this service return session credentials
   * then this method will return {@code empty()}; no attempt is
   * made to connect to STS.
   * Otherwise, the STS binding info will be looked up and an attempt
   * made to connect to STS.
   * Only one attempt will be made.
   * @return any STS client created.
   * @throws IOException any failure to bind to STS.
   */
  private synchronized Optional<STSClientFactory.STSClient> maybeInitSTS()
      throws IOException {
    if (stsInitAttempted) {
      return stsClient;
    }
    stsInitAttempted = true;

    Configuration conf = getConfig();
    URI uri = getCanonicalUri();

    // Ask the owner for any session credentials which it already has
    // so that it can just propagate them.
    // this call may fail if there are no credentials on the auth
    // chain.
    // As no codepath (session propagation, STS creation) will work,
    // throw
    final AWSCredentials parentCred = once("get credentials",
        "",
        () -> parentAuthChain.getCredentials());
    hasSessionCreds = parentCred instanceof AWSSessionCredentials;

    if (!hasSessionCreds) {
      String endpoint = conf.getTrimmed(DELEGATION_TOKEN_ENDPOINT,
          DEFAULT_DELEGATION_TOKEN_ENDPOINT);
      String region = conf.getTrimmed(DELEGATION_TOKEN_REGION, "");
      LOG.debug("Creating STS client for user {}," +
              " endpoint {} and token duration {}",
          getOwner().getShortUserName(), endpoint, duration);

      invoker = new Invoker(new S3ARetryPolicy(conf), Invoker.LOG_EVENT);
      ClientConfiguration awsConf =
          S3AUtils.createAwsConf(conf, uri.getHost());
      AWSSecurityTokenService tokenService =
          STSClientFactory.builder(parentAuthChain,
              awsConf,
              endpoint,
              region)
              .build();
      stsClient = Optional.of(
          STSClientFactory.createClientConnection(tokenService, invoker));
    } else {
      LOG.debug("Parent-provided session credentials will be propagated");
      stsClient = Optional.empty();
    }
    return stsClient;
  }

  /**
   * Get the client to AWS STS.
   * @return the STS client, when successfully inited.
   */
  protected Optional<STSClientFactory.STSClient> prepareSTSClient()
      throws IOException {
    return maybeInitSTS();
  }

  /**
   * Duration of sessions.
   * @return duration in seconds.
   */
  public long getDuration() {
    return duration;
  }

  @Override
  @Retries.RetryTranslated
  public SessionTokenIdentifier createTokenIdentifier(
      final Optional<RoleModel.Policy> policy,
      final EncryptionSecrets encryptionSecrets) throws IOException {
    requireServiceStarted();

    final MarshalledCredentials marshalledCredentials;
    String origin = AbstractS3ATokenIdentifier.createDefaultOriginMessage();
    final Optional<STSClientFactory.STSClient> client = prepareSTSClient();

    if (client.isPresent()) {
      // this is the normal route: ask for a new STS token
      marshalledCredentials = new MarshalledCredentials(
          client.get()
              .requestSessionCredentials(duration, TimeUnit.SECONDS));
    } else {
      // get a new set of parental session credentials (pick up IAM refresh)
      if (!forwardMessageLogged.getAndSet(true)) {
        // warn caller on the first -and only the first- use.
        LOG.warn("Forwarding existing session credentials to {}"
            + " -duration unknown", getCanonicalUri());
      }
      origin += " " + CREDENTIALS_CONVERTED_TO_DELEGATION_TOKEN;
      final AWSCredentials awsCredentials
          = parentAuthChain.getCredentials();
      if (awsCredentials instanceof AWSSessionCredentials) {
        marshalledCredentials = new MarshalledCredentials(
            (AWSSessionCredentials) awsCredentials);
      } else {
        throw new DelegationTokenIOException(
            "Parent auth chain is no longer supplying session secrets");
      }
    }
    return new SessionTokenIdentifier(getKind(),
        getOwnerText(),
        getCanonicalUri(),
        marshalledCredentials,
        encryptionSecrets,
        origin);
  }

  @Override
  public SessionTokenIdentifier createEmptyIdentifier() {
    return new SessionTokenIdentifier();
  }

}
