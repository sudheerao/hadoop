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

package org.apache.hadoop.fs.azure.integration;

import java.io.IOException;

import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.toHuman;

public class ITestAzureInputStreamPerformance
    extends AbstractAzureScaleTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      ITestAzureInputStreamPerformance.class);

  private NativeAzureFileSystem fileSystem;
  private Path testDataPath;
  private FileStatus testDataStatus;
  private FSDataInputStream in;
  public static final int BLOCK_SIZE = _32KB;
  public static final int BIG_BLOCK_SIZE = _256KB;


  /**
   * Open the FS and the test data. The input stream is always set up here.
   * @throws IOException IO Problems.
   */
  @Override
  public void setup() throws Exception {
    super.setup();
    Configuration conf = getConfiguration();
    String testFile = conf.getTrimmed(KEY_CSVTEST_FILE, DEFAULT_CSVTEST_FILE);
    Assume.assumeFalse("Empty test property: " + KEY_CSVTEST_FILE,
        testFile.isEmpty());
    testDataPath = new Path(testFile);
    LOG.info("Using {} as input stream source", testDataPath);
    bind(this.testDataPath);
    try {
      testDataStatus = fileSystem.getFileStatus(this.testDataPath);
    } catch (IOException e) {
      LOG.warn("Failed to read file {} specified in {}",
          testFile, KEY_CSVTEST_FILE, e);
      throw e;
    }
  }

  private void bind(Path path) throws IOException {
    fileSystem = (NativeAzureFileSystem) FileSystem.newInstance(path.toUri(),
        getConfiguration());
  }

  /**
   * Cleanup: close the stream, close the FS.
   */
  @Override
  public void teardown() throws Exception {
    describe("cleanup");
    super.teardown();
    IOUtils.closeStream(in);
    IOUtils.closeStream(fileSystem);
  }

  /**
   * Open the test file with the read buffer size specified in the setting.
   * {@link #KEY_READ_BUFFER_SIZE}.
   * @return the stream
   * @throws IOException IO problems
   */
  FSDataInputStream openTestFile() throws IOException {
    int bufferSize = getConfiguration().getInt(KEY_READ_BUFFER_SIZE,
        DEFAULT_READ_BUFFER_SIZE);
    return fileSystem.open(testDataPath, bufferSize);
  }

  /**
   * Log how long an IOP took, by dividing the total time by the
   * count of operations, printing in a human-readable form.
   * @param operation operation being measured
   * @param timer timing data
   * @param count IOP count.
   */
  protected void logTimePerIOP(String operation,
      ContractTestUtils.NanoTimer timer,
      long count) {
    LOG.info("Time per {}: {} nS",
        operation, toHuman(timer.duration() / count));
  }


  @Test
  public void testOpenAndClose() throws Throwable {
    describe("Open then close %s", testDataPath);
    ContractTestUtils.NanoTimer
        readTimer = new ContractTestUtils.NanoTimer();

    try(FSDataInputStream f = openTestFile()) {
      assertNotEquals(-1, f.read());
    }
    readTimer.end("Open test file and read from offset 0");
  }
}


