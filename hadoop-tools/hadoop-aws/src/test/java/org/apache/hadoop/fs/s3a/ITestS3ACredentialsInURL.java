/**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;

import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.TEST_FS_S3A_NAME;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeS3GuardState;

/**
 * Tests that credentials can go into the URL. This includes a valid
 * set, and a check that an invalid set do at least get stripped out
 * of the final URI
 */
public class ITestS3ACredentialsInURL extends Assert {
  private S3AFileSystem fs;
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ACredentialsInURL.class);
  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @After
  public void teardown() {
    IOUtils.closeStream(fs);
  }

  /**
   * Test instantiation.
   * @throws Throwable
   */
  @Test
  public void testInstantiateFromURL() throws Throwable {

    Configuration conf = new Configuration();

    // Skip in the case of S3Guard with DynamoDB because it cannot get
    // credentials for its own use if they're only in S3 URLs
    assumeS3GuardState(false, conf);
    String fsname = conf.getTrimmed(TEST_FS_S3A_NAME, "");
    URI original = new URI(fsname);
    S3xLoginHelper.Login login = S3AUtils.getAWSAccessKeys(original, conf);
    String accessKey = login.getUser();
    String secretKey = login.getPassword();
    Assume.assumeNotNull(accessKey, secretKey);
    URI secretsURI = S3ATestUtils.createUriWithEmbeddedSecrets(original,
        accessKey, secretKey);
    if (secretKey.contains("/")) {
      assertTrue("test URI encodes the / symbol", secretsURI.toString().
          contains("%252F"));
    }
    if (secretKey.contains("+")) {
      assertTrue("test URI encodes the + symbol", secretsURI.toString().
          contains("%252B"));
    }
    assertFalse("Does not contain secrets", original.equals(secretsURI));

    conf.set(TEST_FS_S3A_NAME, secretsURI.toString());
    conf.unset(Constants.ACCESS_KEY);
    conf.unset(Constants.SECRET_KEY);
    fs = S3ATestUtils.createTestFileSystem(conf);

    String fsURI = fs.getUri().toString();
    assertFalse("FS URI contains a @ symbol", fsURI.contains("@"));
    assertFalse("FS URI contains a % symbol", fsURI.contains("%"));
    if (!original.toString().startsWith(fsURI)) {
      fail("Filesystem URI does not match original");
    }
    validate("original path", new Path(original));
    validate("bare path", new Path("/"));
    validate("secrets path", new Path(secretsURI));
  }

  private void validate(String text, Path path) throws IOException {
    try {
      fs.canonicalizeUri(path.toUri());
      fs.checkPath(path);
      assertTrue(text + " Not a directory",
          fs.getFileStatus(new Path("/")).isDirectory());
      fs.globStatus(path);
    } catch (AssertionError e) {
      throw e;
    } catch (Exception e) {
      LOG.debug("{} failure: {}", text, e, e);
      fail(text + " Test failed");
    }
  }

  /**
   * Set up some invalid credentials, verify login is rejected.
   */
  @Test
  public void testInvalidCredentialsFail() throws Throwable {
    Configuration conf = new Configuration();
    // use the default credential provider chain
    conf.unset(AWS_CREDENTIALS_PROVIDER);
    String fsname = conf.getTrimmed(TEST_FS_S3A_NAME, "");
    Assume.assumeNotNull(fsname);
    assumeS3GuardState(false, conf);
    URI original = new URI(fsname);
    URI testURI = S3ATestUtils.createUriWithEmbeddedSecrets(original, "user", "//");

    conf.set(TEST_FS_S3A_NAME, testURI.toString());
    LambdaTestUtils.intercept(AccessDeniedException.class,
        () -> {
          fs = S3ATestUtils.createTestFileSystem(conf);
          return fs.getFileStatus(new Path("/"));
        });
  }

}
