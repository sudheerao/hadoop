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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.auth.MarshalledCredentials;
import org.apache.hadoop.fs.s3a.auth.STSClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeSessionTestsEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.requestSessionCredentials;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.unsetHadoopCredentialProviders;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests use of temporary credentials (for example, AWS STS & S3).
 *
 * The property {@link Constants#ASSUMED_ROLE_STS_ENDPOINT} can be set to
 * point this at different STS endpoints.
 * This test will use the AWS credentials (if provided) for
 * S3A tests to request temporary credentials, then attempt to use those
 * credentials instead.

 */
public class ITestS3ATemporaryCredentials extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ATemporaryCredentials.class);

  private static final String TEMPORARY_AWS_CREDENTIALS
      = TemporaryAWSCredentialsProvider.NAME;

  private static final long TEST_FILE_SIZE = 1024;

  private AWSCredentialProviderList credentials;

  @Override
  public void setup() throws Exception {
    super.setup();
    assumeSessionTestsEnabled(getConfiguration());
  }

  @Override
  public void teardown() throws Exception {
    S3AUtils.closeAutocloseables(LOG, credentials);
    super.teardown();
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.setBoolean(DELEGATION_TOKENS_ENABLED, true);
    return conf;
  }

  /**
   * Test use of STS for requesting temporary credentials.
   *
   * The property test.sts.endpoint can be set to point this at different
   * STS endpoints. This test will use the AWS credentials (if provided) for
   * S3A tests to request temporary credentials, then attempt to use those
   * credentials instead.
   *
   * @throws IOException failure
   */
  @Test
  public void testSTS() throws IOException {
    Configuration conf = getContract().getConf();
    S3AFileSystem testFS = getFileSystem();
    credentials = testFS.shareCredentials("testSTS");

    String bucket = testFS.getBucket();
    AWSSecurityTokenServiceClientBuilder builder = STSClientFactory.builder(
        conf,
        bucket,
        credentials,
        getStsEndpoint(conf),
        getStsRegion(conf));
    STSClientFactory.STSClient clientConnection =
        STSClientFactory.createClientConnection(
            builder.build(),
            new Invoker(new S3ARetryPolicy(conf), Invoker.LOG_EVENT));
    Credentials sessionCreds = clientConnection
        .requestSessionCredentials(900, TimeUnit.SECONDS);

    // clone configuration so changes here do not affect the base FS.
    Configuration conf2 = new Configuration(conf);
    S3AUtils.clearBucketOption(conf2, bucket, AWS_CREDENTIALS_PROVIDER);
    S3AUtils.clearBucketOption(conf2, bucket, ACCESS_KEY);
    S3AUtils.clearBucketOption(conf2, bucket, SECRET_KEY);
    S3AUtils.clearBucketOption(conf2, bucket, SESSION_TOKEN);

    updateConfigWithSessionCreds(conf2,
        new MarshalledCredentials(sessionCreds));

    conf2.set(AWS_CREDENTIALS_PROVIDER, TEMPORARY_AWS_CREDENTIALS);

    // with valid credentials, we can set properties.
    try(S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf2)) {
      createAndVerifyFile(fs, path("testSTS"), TEST_FILE_SIZE);
    }

    // now create an invalid set of credentials by changing the session
    // token
    conf2.set(SESSION_TOKEN, "invalid-" + sessionCreds.getSessionToken());
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf2)) {
      createAndVerifyFile(fs, path("testSTSInvalidToken"), TEST_FILE_SIZE);
      fail("Expected an access exception, but file access to "
          + fs.getUri() + " was allowed: " + fs);
    } catch (AWSS3IOException | AWSBadRequestException ex) {
      LOG.info("Expected Exception: {}", ex.toString());
      LOG.debug("Expected Exception: {}", ex, ex);
    }
  }

  protected String getStsEndpoint(final Configuration conf) {
    return conf.getTrimmed(ASSUMED_ROLE_STS_ENDPOINT,
            DEFAULT_ASSUMED_ROLE_STS_ENDPOINT);
  }

  protected String getStsRegion(final Configuration conf) {
    return conf.getTrimmed(ASSUMED_ROLE_STS_ENDPOINT_REGION,
        ASSUMED_ROLE_STS_ENDPOINT_REGION_DEFAULT);
  }

  @Test
  public void testTemporaryCredentialValidation() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(ACCESS_KEY, "accesskey");
    conf.set(SECRET_KEY, "secretkey");
    conf.set(SESSION_TOKEN, "");
    LambdaTestUtils.intercept(CredentialInitializationException.class,
        () -> new TemporaryAWSCredentialsProvider(conf).getCredentials());
  }

  /**
   * Test STS binding.
   */
  @Test
  public void testSTSBindingforGetDelegationToken() throws Exception {
    Configuration conf = new Configuration(getContract().getConf());
    MarshalledCredentials sc = requestSessionCredentials(conf,
        getFileSystem().getBucket());
    sc.getCredentials();
    updateConfigWithSessionCreds(conf, sc);
    conf.set(AWS_CREDENTIALS_PROVIDER, TEMPORARY_AWS_CREDENTIALS);

    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      createAndVerifyFile(fs, path("testSTS"), TEST_FILE_SIZE);
      intercept(AccessDeniedException.class,
          () -> fs.getDelegationToken(""));
    }
  }

  protected void updateConfigWithSessionCreds(final Configuration conf,
      final MarshalledCredentials sc) {
    unsetHadoopCredentialProviders(conf);
    sc.setSecretsInConfiguration(conf);
  }

  /**
   * Verify that asking for a delegation token will be rejected by AWS.
   */
  @Test
  public void testNoDelegationTokenIssue() throws Exception {
    Configuration conf = new Configuration(getContract().getConf());
    MarshalledCredentials sc = requestSessionCredentials(conf,
        getFileSystem().getBucket());
    sc.getCredentials();
    updateConfigWithSessionCreds(conf, sc);
    conf.set(AWS_CREDENTIALS_PROVIDER, TEMPORARY_AWS_CREDENTIALS);

    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      intercept(AccessDeniedException.class,
          () -> fs.getDelegationToken(""));
    }
  }

  /**
   * Create an invalid session token and verify that it is rejected.
   */
  @Test
  public void testInvalidSTSBinding() throws Exception {
    Configuration conf = new Configuration(getContract().getConf());

    MarshalledCredentials sc = requestSessionCredentials(conf,
        getFileSystem().getBucket());
    sc.getCredentials();
    updateConfigWithSessionCreds(conf, sc);

    conf.set(AWS_CREDENTIALS_PROVIDER, TEMPORARY_AWS_CREDENTIALS);
    conf.set(SESSION_TOKEN, "invalid-" + sc.getSessionToken());
    S3AFileSystem fs = null;

    try {
      // this may throw an exception, which is an acceptable outcome.
      // it must be in the try/catch clause.
      fs = S3ATestUtils.createTestFileSystem(conf);
      Path path = path("testSTSInvalidToken");
      createAndVerifyFile(fs,
          path,
            TEST_FILE_SIZE);
      // this is a failure path, so fail with a meaningful error
      fail("request to create a file should have failed");
    } catch (AWSBadRequestException expected){
      // likely at two points in the operation, depending on
      // S3Guard state
    } finally {
      IOUtils.closeStream(fs);
    }
  }

  @Test
  public void testTemporaryCredentialValidationOnLoad() throws Throwable {
    Configuration conf = new Configuration();
    unsetHadoopCredentialProviders(conf);
    conf.set(ACCESS_KEY, "");
    conf.set(SECRET_KEY, "");
    conf.set(SESSION_TOKEN, "");
    final MarshalledCredentials sc = MarshalledCredentials.load(null, conf);
    intercept(IOException.class,
        MarshalledCredentials.INVALID_CREDENTIALS,
        () -> {
          sc.validate("", true);
          return sc.toString();
        });
  }

  /**
   * Verify that the request mechanism is translating exceptions.
   * @throws Exception on a failure
   */
  @Test
  public void testSessionRequestExceptionTranslation() throws Exception {
    intercept(IOException.class,
        () -> requestSessionCredentials(getConfiguration(),
            getFileSystem().getBucket(), 10));
  }

}
