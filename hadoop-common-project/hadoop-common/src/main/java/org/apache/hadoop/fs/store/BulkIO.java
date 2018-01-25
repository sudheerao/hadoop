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

package org.apache.hadoop.fs.store;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

/**
 * Interface for a filesystem which supports BulkIO.
 * This is only for DistCP; a public API may be completely different.
 */
@InterfaceAudience.LimitedPrivate("DistCP")
@InterfaceStability.Unstable
public interface BulkIO {

  /**
   * Return the maximum delete page size.
   * @return a value greater than 0 if bulk delete is supported.
   */
  int getBulkDeleteLimit();

  /**
   * Initiate a bulk delete operation.
   * <ol>
   *   <li>This is not expected to be atomic.</li>
   *   <li>If a failure occurs, the outcome of the overall operation is
   *   undefined.</li>
   *   <li>It is not required to be O(1), only that it should scale better.</li>
   *   <li>There's no guarantee that for small lists, it is any faster at all.</li>
   * </ol>
   *
   * @param pathsToDelete (possibly empty) list of paths to delete
   * @return the number of entries deleted.
   * @throws IOException a failure.
   */
  int bulkDelete(List<Path> pathsToDelete) throws IOException;
}
