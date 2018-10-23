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

import java.net.URI;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AEncryptionMethods;
import org.apache.hadoop.fs.s3a.S3ATestConstants;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.test.HadoopTestBase;

/**
 * Unit test of session credential support.
 */
public class TestMarshalledCredentials extends HadoopTestBase {

  private MarshalledCredentials credentials;

  private int expiration;

  private static URI landsatUri;


  @BeforeClass
  public static void classSetup() throws Exception {
    landsatUri = new URI(S3ATestConstants.DEFAULT_CSVTEST_FILE);
  }

  @Before
  public void createSessionToken() {
    credentials = new MarshalledCredentials("accessKey",
        "secretKey", "sessionToken");
    credentials.setRoleARN("roleARN");
    expiration = 1970;
    credentials.setExpiration(expiration);
  }

  @Test
  public void testRoundTrip() throws Throwable {
    MarshalledCredentials c2 = S3ATestUtils.roundTrip(this.credentials,
        new Configuration());
    assertEquals(credentials, c2);
    assertEquals("accessKey", c2.getAccessKey());
    assertEquals("secretKey", c2.getSecretKey());
    assertEquals("sessionToken", c2.getSessionToken());
    assertEquals(expiration, c2.getExpiration());
    assertEquals(credentials, c2);
  }

  @Test
  public void testRoundTripNoSessionData() throws Throwable {
    MarshalledCredentials c = new MarshalledCredentials();
    c.setAccessKey("A");
    c.setSecretKey("K");
    MarshalledCredentials c2 = S3ATestUtils.roundTrip(c,
        new Configuration());
    assertEquals(c, c2);
  }

  @Test
  public void testRoundTripEncryptionData() throws Throwable {
    EncryptionSecrets secrets = new EncryptionSecrets(
        S3AEncryptionMethods.SSE_KMS,
        "key");
    EncryptionSecrets result = S3ATestUtils.roundTrip(secrets,
        new Configuration());
    assertEquals(secrets, result);
  }

}
