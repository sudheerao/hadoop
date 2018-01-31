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
   * Return the maximum bulk delete page size.
   * Calling {@link #bulkDeleteFiles(List)} with a list size greater
   * than this limit will be rejected.
   * @return a value greater than 0 if bulk delete is supported.
   */
  int getBulkDeleteFilesLimit();

  /**
   * Initiate a bulk delete operation.
   *
   * Preconditions:
   * <pre>
   *   files == list of Path
   *   forall f in files: not exists(FS, f) or isFile(FS, f)
   * </pre>
   *
   * Postconditions for a successfully completed operation
   * <pre>
   *   FS' = FS where forall f in files:
   *      not exists(FS', f) and isDirectory(FS', parent(f))
   * </pre>
   *
   * <ol>
   *   <i>All paths in the list which resolve to a entry
   *   MUST refer to files.</i>
   *   <li>If a directory is included in the list, the outcome
   *   is undefined.</li>
   *   <li>The operation is unlikely  be atomic.</li>
   *   <li>If an error occurs, the state of the filesystem is undefined.
   *   Some, all or none of the other files may have been deleted.</li>
   *   <li>It is not expected that the changes in the operation
   *   will be isolated from other, concurrent changes to the FS.</li>
   *   <li>Duplicates may result in multiple attempts to delete the file,
   *   or they may be filtered.</li>
   *   <li>It is not required to be O(1), only that it should scale better.
   *   than a sequence of individual file delete operations.</li>
   *   <li>There's no guarantee that for small sets of files bulk deletion
   *   is faster than single deletes. It may even be slower.</li>
   *   <li>It is not an error if a listed file does not exist.</li>
   *   <li>If a path which does not exist is deleted, the filesystem
   *   <i>may</i> still create a fake directory marker where its
   *   parent directory was.</li>
   * </ol>
   * The directory marker is relevant for object stores which create them.
   * For performance, the list of files to create may not be probed before
   * a bulk delete request is issued, yet afterwards the store is
   * expected to contain the parent directories, if present.
   * Accordingly, an implementation may create an empty marker dir for all
   * paths passed in, even if they don't refer to files.
   * @param filesToDelete (possibly empty) list of files to delete
   * @return the number of files included in the filesystem delete request after
   * duplicates were discarded.
   * @throws IOException IO failure.
   * @throws IllegalArgumentException precondition failure
   */
  int bulkDeleteFiles(List<Path> filesToDelete) throws IOException;
}
