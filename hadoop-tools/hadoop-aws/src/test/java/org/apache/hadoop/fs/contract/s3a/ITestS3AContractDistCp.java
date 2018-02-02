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

package org.apache.hadoop.fs.contract.s3a;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.SCALE_TEST_TIMEOUT_MILLIS;


/**
 * Contract test suite covering S3A integration with DistCp.
 * Uses the block output stream, buffered to disk. This is the
 * recommended output mechanism for DistCP due to its scalability.
 */
public class ITestS3AContractDistCp extends AbstractContractDistCpTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AContractDistCp.class);
  private static final long MULTIPART_SETTING = MULTIPART_MIN_SIZE;

  @Override
  protected int getTestTimeoutMillis() {
    return SCALE_TEST_TIMEOUT_MILLIS;
  }

  /**
   * Create a configuration, possibly patching in S3Guard options.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration newConf = super.createConfiguration();
    newConf.setLong(MULTIPART_SIZE, MULTIPART_SETTING);
    // set a small page size to force distcp into multipage bulk IO
    newConf.setInt(EXPERIMENTAL_BULKDELETE_PAGESIZE, 2);
    // patch in S3Guard options
    maybeEnableS3Guard(newConf);
    return newConf;
  }

  @Override
  protected S3AContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  /**
   * Wrap the update code with an assertion to verify that the
   * bulk delete call was called at least once.
   * This is needed to verify that the new delete path was used
   * @param outputDir output directory used by the initial distcp
   */
  @Override
  protected void updateDeepDirectoryStructure(final Path outputDir)
      throws Exception {
    S3AFileSystem remoteFS = (S3AFileSystem) getRemoteFS();
    MetricDiff bulkDeleteDiff = new MetricDiff(remoteFS,
        Statistic.INVOCATION_BULK_DELETE);
    super.updateDeepDirectoryStructure(outputDir);
    long bulkInvocations = bulkDeleteDiff.diff();
    Assert.assertNotEquals("Bulk Delete not invoked on " + remoteFS,
        0, bulkInvocations);
    LOG.info("FS summary: {}", remoteFS);
  }
}
