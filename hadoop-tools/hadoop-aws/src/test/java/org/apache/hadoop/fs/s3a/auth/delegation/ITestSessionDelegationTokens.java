/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.File;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.auth.SessionCredentials;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeSessionTestsEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.roundTrip;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.unsetHadoopCredentialProviders;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_SESSION_BINDING;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.SESSION_TOKEN_KIND;
import static org.apache.hadoop.fs.s3a.auth.delegation.SessionTokenBinding.CREDENTIALS_CONVERTED_TO_DELEGATION_TOKEN;
import static org.apache.hadoop.test.GenericTestUtils.notNull;

/**
 * Tests use of Hadoop delegation tokens to marshall S3 credentials.
 *
 */
public class ITestSessionDelegationTokens extends AbstractDelegationIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestSessionDelegationTokens.class);

  private S3ADelegationTokens delegationTokens;


  /**
   * Get the delegation token binding for this test suite.
   * @return which DT binding to use.
   */
  protected String getDelegationBinding() {
    return DELEGATION_TOKEN_SESSION_BINDING;
  }

  public Text getTokenKind() {
    return SESSION_TOKEN_KIND;
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    enableDelegationTokens(conf, getDelegationBinding());
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeSessionTestsEnabled(getConfiguration());
    resetUGI();
    Configuration conf = getConfiguration();
    S3AFileSystem fs = getFileSystem();
    delegationTokens = new S3ADelegationTokens();
    delegationTokens.bindToFileSystem(fs.getCanonicalUri(), fs);
    delegationTokens.init(conf);
    delegationTokens.start();
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.cleanupWithLogger(LOG, delegationTokens);
    resetUGI();
    super.teardown();
  }

  @Test
  public void testSaveLoadTokens() throws Throwable {
    File tokenFile = File.createTempFile("token", "bin");
    Token<AbstractS3ATokenIdentifier> dt
        = delegationTokens.createDelegationToken(new EncryptionSecrets());
    final AbstractS3ATokenIdentifier origIdentifier
        = dt.decodeIdentifier();
    assertEquals("kind in " + dt, getTokenKind(), dt.getKind());
    Configuration conf = getConfiguration();
    saveDT(tokenFile, dt);
    assertTrue("Empty token file", tokenFile.length() > 0);
    Credentials creds = Credentials.readTokenStorageFile(tokenFile, conf);
    Text serviceId = delegationTokens.getService();
    Token<? extends TokenIdentifier> token =
        notNull("No token for \"" + serviceId + "\"" +
                " in: " + creds.getAllTokens(),
        creds.getToken(serviceId));
    AbstractS3ATokenIdentifier dtId =
        (AbstractS3ATokenIdentifier) token.decodeIdentifier();
    dtId.validate();
    assertEquals("token identifier ", origIdentifier, dtId);
    assertEquals("Origin in " + dtId,
        origIdentifier.getOrigin(), dtId.getOrigin());
  }

  /**
   * This creates a DT from a set of credentials, then verifies
   * that you can use the round-tripped credentials as a source of
   * authentication for another DT binding, and when
   * that is asked for a DT token, the secrets it returns are
   * the same as the original.
   *
   * That is different from DT propagation, as here the propagation
   * is by setting the fs.s3a session/secret/id keys from the marshalled
   * values, and using session token auth.
   * This verifies that session token authentication can be used
   * for DT credential auth, and that new tokens aren't created.
   *
   * From a testing perspective, this is not as "good" as having
   * separate tests, but given the effort to create session tokens
   * is all hidden in the first FS, it is actually easier to write
   * and now forms an extra test on those generated tokens as well
   * as the marshalling.
   */
  @Test
  public void testCreateAndUseDT() throws Throwable {
    describe("Create a Delegation Token, round trip then reuse");

    final S3AFileSystem fs = getFileSystem();
    final Configuration conf = fs.getConf();

    assertNull("Current User has delegation token",
        delegationTokens.selectTokenFromActiveUser());
    Token<AbstractS3ATokenIdentifier> dt
        = delegationTokens.createDelegationToken(
            new EncryptionSecrets(S3AEncryptionMethods.SSE_KMS, "kms key"));
    assertEquals("Token kind mismatch", getTokenKind(), dt.getKind());

    assertNotNull("Could not bind to an existing DT",
        delegationTokens.bindToDT(dt));
    final AWSSessionCredentials awsSessionCreds
        = verifySessionCredentials(
        delegationTokens.createCredentialProvider().getCredentials());
    final SessionCredentials origCreds = new SessionCredentials(
        awsSessionCreds);

    // simulate marshall and transmission
    final SessionCredentials creds = roundTrip(origCreds, conf);

    // decode to get the binding info
    AbstractS3ATokenIdentifier dtId =
        notNull("no identifier in " + dt, dt.decodeIdentifier());
    dtId.validate();

    // now use those chained credentials to create a new FS instance
    // and then get a session DT from it and expect equality
    verifyAWSSessionCredentialPropagation(fs, creds, new Configuration(conf));
  }

  /**
   * This verifies that AWS Session credentials can be picked up and
   * returned in a DT.
   * With a session binding, this holds; for role binding it will fail.
   * @param fs base FS to bond to.
   * @param session session credentials from first DT.
   * @param conf config to use
   * @return the retrieved DT. This is only for error reporting.
   * @throws IOException failure.
   */
  protected AbstractS3ATokenIdentifier verifyAWSSessionCredentialPropagation(
      final S3AFileSystem fs,
      final SessionCredentials session,
      final Configuration conf)
      throws Exception {
    describe("Verify Token Propagation");
    // clear any cred paths to ensure they don't get picked up if used for auth.
    unsetHadoopCredentialProviders(conf);
    conf.set(ASSUMED_ROLE_CREDENTIALS_PROVIDER,
        TemporaryAWSCredentialsProvider.NAME);
    session.setSecretsInConfiguration(conf);
    try(S3ADelegationTokens delegationTokens2 = new S3ADelegationTokens()) {
      delegationTokens2.bindToFileSystem(fs.getCanonicalUri(), fs);
      delegationTokens2.init(conf);
      delegationTokens2.start();

      final Token<AbstractS3ATokenIdentifier> newDT
          = delegationTokens2.getBoundOrNewDT(new EncryptionSecrets());
      final AbstractS3ATokenIdentifier boundId
          = delegationTokens2.bindToDT(newDT);

      LOG.info("Regenerated DT is {}", newDT);
      final SessionCredentials creds2 = new SessionCredentials(
          verifySessionCredentials(
              delegationTokens2.createCredentialProvider().getCredentials()));
      assertEquals("Credentials", session, creds2);
      assertTrue("Origin in " + boundId,
          boundId.getOrigin()
              .contains(CREDENTIALS_CONVERTED_TO_DELEGATION_TOKEN));
      return boundId;
    }
  }

  private AWSSessionCredentials verifySessionCredentials(
      final AWSCredentials creds) {
    AWSSessionCredentials session = (AWSSessionCredentials) creds;
    assertNotNull("access key", session.getAWSAccessKeyId());
    assertNotNull("secret key", session.getAWSSecretKey());
    assertNotNull("session token", session.getSessionToken());
    return session;
  }

}
