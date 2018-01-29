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
  int getBulkDeleteFilesLimit();

  /**
   * Initiate a bulk delete operation.
   *
   * Preconditions:
   * <pre>
   *   files = list of Path
   *   forall f in files: not exists(FS, f) or isFile(FS, f)
   * </pre>
   *
   * Postconditions for a successfully completed operation
   * <pre>
   *   FS' = FS where not exists(FS', f) forall f in files
   * </pre>
   *
   * <ol>
   *   <i>All paths in the list must consist of files.</i>
   *   <li>If a directory is included in the list, the outcome will be one of:
   *   reject, ignore.</li>
   *   <li>The operation must not be expected to be atomic.</li>
   *   <li>If an error occurs, the state of the filesystem is undefined.
   *   Some, all or none of the other files may have been deleted.</li>
   *   <li>It is not required to be O(1), only that it should scale better.
   *   than a sequence of individual file delete operations.</li>
   *   <li>It is not expected that the changes in the operation
   *   will be isolated from other, concurrent changes to the FS.</li>
   *   <li>Duplicates may result in multiple attempts to delete the file,
   *   or they may be filtered.</li>
   *   <li>There's no guarantee that for small sets of files, it is any
   *   faster at all.</li>
   * </ol>
   *
   * @param filesToDelete (possibly empty) list of files to delete
   * @return the number of files deleted.
   * @throws IOException a failure.
   */
  int bulkDeleteFiles(List<Path> filesToDelete) throws IOException;
}
