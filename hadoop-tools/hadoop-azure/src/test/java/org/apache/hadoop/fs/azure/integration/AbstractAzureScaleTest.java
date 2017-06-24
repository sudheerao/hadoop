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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.azure.integration.AzureTestUtils.*;

/**
 * Scale tests are only executed if the scale profile
 * is set; the setup method will check this and skip
 * tests if not.
 *
 * The S3A test has a very complex setup related to configuration of
 * test timeouts and scalability thereof. Ideally, that should be avoided
 * here.
 */
public abstract class AbstractAzureScaleTest
    extends AbstractAzureIntegrationTest implements Sizes {

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractAzureScaleTest.class);
  private boolean enabled;

  @Override
  protected int getTestTimeoutMillis() {
    return AzureTestConstants.SCALE_TEST_TIMEOUT_MILLIS;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    LOG.debug("Scale test operation count = {}", getOperationCount());
    enabled = getTestPropertyBool(
        getConfiguration(),
        KEY_SCALE_TESTS_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED);
    assume("Scale test disabled: to enable set property "
            + KEY_SCALE_TESTS_ENABLED,
        isEnabled());
  }

  /**
   * Is the test enabled. Base implementation looks at the scale property;
   * subclasses may add extra criteria. They <i>must</i> always include
   * the state of the base implementation as part of the requirements
   * which must be true for the test boe considered enabled.
   * @return true if the test is enabled and so can be executed.
   */
  protected boolean isEnabled() {
    return enabled;
  }

  protected long getOperationCount() {
    return getConfiguration().getLong(KEY_OPERATION_COUNT,
        DEFAULT_OPERATION_COUNT);
  }
}
