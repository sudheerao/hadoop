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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.CredentialInitializationException;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenIOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.ProviderUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SESSION_TOKEN;
import static org.apache.hadoop.fs.s3a.S3AUtils.lookupPassword;

/**
 * Stores the credentials for a session or for a full login.
 * This structure is {@link Writable}, so can be marshalled inside a
 * delegation token.
 */
@InterfaceAudience.Private
public final class MarshalledCredentials implements Writable,
    AWSCredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(
      MarshalledCredentials.class);

  public static final String INVALID_CREDENTIALS
      = "Invalid credentials";

  /**
   * How long can any of the secrets be.
   * This is much longer than the current tokens, but leaves space for
   * future enhancements.
   */
  private static final int MAX_SECRET_LENGTH = 4096;

  private String accessKey;

  private String secretKey;

  private String sessionToken = "";

  private String roleARN = "";

  private long expiration;

  public MarshalledCredentials() {
  }

  /**
   * Instantiate from a set of credentials issued by an STS call.
   * @param credentials AWS-provided session credentials
   */
  public MarshalledCredentials(final Credentials credentials) {
    accessKey = credentials.getAccessKeyId();
    secretKey = credentials.getSecretAccessKey();
    sessionToken = credentials.getSessionToken();
    expiration = credentials.getExpiration().getTime();
  }

  /**
   * Create from a set of properties.
   * No expiry time is expected/known here.
   * @param accessKey access key
   * @param secretKey secret key
   * @param sessionToken session token
   */
  public MarshalledCredentials(final String accessKey,
      final String secretKey,
      final String sessionToken) {
    this.accessKey = checkNotNull(accessKey);
    this.secretKey = checkNotNull(secretKey);
    this.sessionToken = checkNotNull(sessionToken);
  }

  /**
   * Create from a set of AWS session credentials.
   * @param awsCredentials the AWS Session info.
   */
  public MarshalledCredentials(final AWSSessionCredentials awsCredentials) {
    this.accessKey = awsCredentials.getAWSAccessKeyId();
    this.secretKey = awsCredentials.getAWSSecretKey();
    this.sessionToken = awsCredentials.getAWSSecretKey();
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  /**
   * Expiration; will be 0 for none known.
   * @return any expiration timestamp
   */
  public long getExpiration() {
    return expiration;
  }

  public void setExpiration(final long expiration) {
    this.expiration = expiration;
  }

  public String getRoleARN() {
    return roleARN;
  }

  public void setRoleARN(String roleARN) {
    this.roleARN = roleARN;
  }

  public void setAccessKey(final String accessKey) {
    this.accessKey = accessKey;
  }

  public void setSecretKey(final String secretKey) {
    this.secretKey = secretKey;
  }

  public void setSessionToken(final String sessionToken) {
    this.sessionToken = sessionToken;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MarshalledCredentials that = (MarshalledCredentials) o;
    return expiration == that.expiration &&
        Objects.equals(accessKey, that.accessKey) &&
        Objects.equals(secretKey, that.secretKey) &&
        Objects.equals(sessionToken, that.sessionToken) &&
        Objects.equals(roleARN, that.roleARN);
  }

  @Override
  public int hashCode() {

    return Objects.hash(accessKey, secretKey, sessionToken, roleARN,
        expiration);
  }


  /**
   * Loads the credentials from the owning FS.
   * There is no validation.
   * @param conf configuration to load from
   * @throws IOException on any load failure
   */
  public static MarshalledCredentials load(
      final URI uri,
      final Configuration conf) throws IOException {
    // determine the bucket
    String bucket = uri != null ? uri.getHost() : "";
    Configuration leanConf =
        ProviderUtils.excludeIncompatibleCredentialProviders(
            conf, S3AFileSystem.class);
    String accessKey = lookupPassword(bucket, leanConf, ACCESS_KEY);
    String secretKey = lookupPassword(bucket, leanConf, SECRET_KEY);
    String sessionToken = lookupPassword(bucket, leanConf, SESSION_TOKEN);
    MarshalledCredentials credentials = new MarshalledCredentials(accessKey,
        secretKey, sessionToken);
    return credentials;
  }

  /**
   * Request a set of credentials from an STS endpoint.
   * @param parentCredentials the parent credentials needed to talk to STS
   * @param stsEndpoint an endpoint, use "" for none
   * @param stsRegion region; use if the endpoint isn't the AWS default.
   * @param duration duration of the credentials in seconds. Minimum value: 900.
   * @param invoker invoker to use for retrying the call.
   * @return the credentials
   * @throws IOException on a failure of the request
   */
  @Retries.RetryTranslated
  public static MarshalledCredentials requestSessionCredentials(
      final AWSCredentialsProvider parentCredentials,
      final ClientConfiguration awsConf,
      final String stsEndpoint,
      final String stsRegion,
      final int duration,
      final Invoker invoker) throws IOException {
    AWSSecurityTokenService tokenService =
        STSClientFactory.builder(parentCredentials,
            awsConf,
            stsEndpoint.isEmpty() ? null : stsEndpoint,
            stsRegion)
            .build();
    STSClientFactory.STSClient clientConnection
        = STSClientFactory.createClientConnection(tokenService, invoker);
    Credentials credentials = clientConnection
        .requestSessionCredentials(duration, TimeUnit.SECONDS);
    return new MarshalledCredentials(credentials);
  }

  /**
   * String value does not include
   * any secrets.
   * @return a string value for logging.
   */
  @Override
  public String toString() {
    return String.format(
        "Session Credentials (%s) for user %s%s; role=%s",
        isValid() ? "valid" : "invalid",
        accessKey,
        (expiration == 0)
            ? ""
            : (" expires " + (new Date(expiration * 1000))),
        roleARN);
  }

  /**
   * Is this a valid set of credentials tokens?
   * @return true if the key and secrets are set.
   */
  public boolean isValid() {
    return isValid(false);
  }

  /**
   * Is this a valid set of credentials tokens?
   * @param sessionTokenRequired is a session token required?
   * @return true if all the fields are set.
   */
  public boolean isValid(boolean sessionTokenRequired) {
    return !StringUtils.isEmpty(accessKey)
        && !StringUtils.isEmpty(secretKey)
        && (!sessionTokenRequired == !StringUtils.isNotEmpty(sessionToken));
  }

  /**
   * Write the token.
   * Only works if valid.
   * @param out stream to serialize to.
   * @throws IOException if the serialization failed.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    validate("Writing " + this + ": ", false);
    Text.writeString(out, accessKey);
    Text.writeString(out, secretKey);
    Text.writeString(out, sessionToken);
    Text.writeString(out, roleARN);
    out.writeLong(expiration);
  }

  /**
   * Read in the fields.
   * @throws IOException IO problem
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    accessKey = Text.readString(in, MAX_SECRET_LENGTH);
    secretKey = Text.readString(in, MAX_SECRET_LENGTH);
    sessionToken = Text.readString(in, MAX_SECRET_LENGTH);

    roleARN = Text.readString(in, MAX_SECRET_LENGTH);
    expiration = in.readLong();
  }

  /**
   * Verify that a set of credentials is valid.
   * @throws DelegationTokenIOException if they aren't
   * @param message message to prefix errors;
   * @param sessionTokenRequired is a session token required?
   */
  public void validate(final String message,
      final boolean sessionTokenRequired) throws IOException {
    if (!isValid(sessionTokenRequired)) {
      throw new DelegationTokenIOException(message
          + INVALID_CREDENTIALS
          + " in " + this);
    }
  }

  /**
   * Create an AWS credential set from these values.
   * From {@code AWSCredentialProvider}.
   * @return a new set of credentials
   * @throws CredentialInitializationException init problems
   */
  @Override
  public AWSCredentials getCredentials() throws
      CredentialInitializationException {
    return toAWSCredentials(true);
  }

  /**
   * From {@code AWSCredentialProvider}.
   * No-op.
   */
  @Override
  public void refresh() {
    /* No-Op */
  }

  /**
   * Create an AWS credential set from these values.
   * @param sessionCredentials is a session token required.
   * @return a new set of credentials
   * @throws CredentialInitializationException validation failure
   */
  public AWSCredentials toAWSCredentials(final boolean sessionCredentials)
      throws CredentialInitializationException {
    if (!isValid(sessionCredentials)) {
      throw new CredentialInitializationException(INVALID_CREDENTIALS);
    }
    if (StringUtils.isNotEmpty(sessionToken)) {
      // a session token was supplied, so return session credentials
      return new BasicSessionCredentials(accessKey, secretKey, sessionToken);
    } else {
      // these are full credentials
      return new BasicAWSCredentials(accessKey, secretKey);
    }
  }

  /**
   * Patch a configuration with the secrets.
   * This does not set any per-bucket options (it doesn't know the bucket...).
   * <i>Warning: once done the configuration must be considered sensitive.</i>
   * @param config configuration to patch
   */
  public void setSecretsInConfiguration(Configuration config) {
    config.set(ACCESS_KEY, accessKey);
    config.set(SECRET_KEY, secretKey);
    S3AUtils.setIfDefined(config, SESSION_TOKEN, sessionToken,
        "session credentials");
  }


  /**
   * Build a set of credentials from the environment.
   * @param env environment.
   * @return a possibly incomplete/invalid set of credentials.
   */
  public static MarshalledCredentials fromEnvironment(
      final Map<String, String> env) {
    MarshalledCredentials creds = new MarshalledCredentials();
    creds.setAccessKey(env.get("AWS_ACCESS_KEY_ID"));
    creds.setSecretKey(env.get("AWS_SECRET_ACCESS_KEY"));
    creds.setSessionToken(env.get("AWS_SESSION_TOKEN"));
    return creds;
  }
}
