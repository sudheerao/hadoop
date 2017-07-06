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

package org.apache.hadoop.fs.azure;

import java.io.IOException;

import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.testBlobPath;
import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.testPath;
import static org.junit.Assume.assumeNotNull;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.integration.AzureTestConstants;

import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.*;

/**
 * Abstract test class that provides basic setup and teardown of testing Azure
 * Storage account.  Each subclass defines a different set of test cases to run
 * and overrides {@link #createTestAccount()} to set up the testing account used
 * to run those tests.  The returned account might integrate with Azure Storage
 * directly or it might be a mock implementation.
 */
public abstract class AbstractWasbTestBase {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractWasbTestBase.class);

  @VisibleForTesting
  protected NativeAzureFileSystem fs;
  protected AzureBlobStorageTestAccount testAccount;

  @Before
  public void setUp() throws Exception {
    testAccount = createTestAccount();
    assumeNotNull(testAccount);
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }
  }

  @After
  public void tearDown() throws Exception {
    testAccount = cleanupTestAccount(testAccount);
    fs = null;
  }

  public Configuration getConfiguration() {
    return new Configuration();
  }

  protected abstract AzureBlobStorageTestAccount createTestAccount()
      throws Exception;

  protected AzureBlobStorageTestAccount getTestAccount() {
    return testAccount;
  }

  protected NativeAzureFileSystem getFileSystem() {
    return fs;
  }

  /**
   * Return a path to a blob which will be unique for this fork.
   * @param filepath filepath
   * @return a path under the default blob directory
   * @throws IOException
   */
  protected Path blobPath(String filepath) throws IOException {
    return testBlobPath(getFileSystem(), filepath);
  }


  /**
   * Create a path under the test path provided by
   * the FS contract.
   * @param filepath path string in
   * @return a path qualified by the test filesystem
   * @throws IOException IO problems
   */
  protected Path path(String filepath) throws IOException {
    return testPath(getFileSystem(), filepath);
  }

  /**
   * Return a path bonded to this method name, unique to this fork during
   * parallel execution.
   * @return a method name unique to (fork, method).
   * @throws IOException IO problems
   */
  protected Path methodPath() throws IOException {
    return path(methodName.getMethodName());
  }

  @Rule
  public TestName methodName = new TestName();


  @BeforeClass
  public static void nameTestThread() {
    Thread.currentThread().setName("JUnit");
  }
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(getTestTimeoutMillis());

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit-" + methodName.getMethodName());
  }

  protected int getTestTimeoutMillis() {
    return AzureTestConstants.AZURE_TEST_TIMEOUT;
  }
}
