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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;

import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.MagicCommitPaths;
import org.apache.hadoop.fs.store.BulkIO;

import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;

/**
 * This performs the bulk IO so that
 */
class S3ABulkOperations implements BulkIO {

  private static final Logger LOG = S3AFileSystem.LOG;

  private final S3AFileSystem owner;

  public S3ABulkOperations(final S3AFileSystem owner) {
    this.owner = owner;
  }

  @Override
  public int getBulkDeleteLimit() {
    return owner.getBulkDeleteLimit();
  }

  @Retries.RetryTranslated
  private int maybeMkdirLeafNodes(PathTreeEntry pathTreeEntry)
      throws IOException {
    List<Path> paths = new ArrayList<>(getBulkDeleteLimit());
    pathTreeEntry.enumLeafNodes(paths);
    LOG.info("Found {} directories to maybe create", paths.size());
    int actualCount = 0;
    for (Path path : paths) {
      if (owner.createFakeDirectoryIfNecessary(path)) {
        actualCount++;
      }
    }
    LOG.info("Created {} directories", actualCount);
    return actualCount;
  }

  @Override // BulkIO
  @Retries.RetryTranslated
  public int bulkDelete(final List<Path> pathsToDelete) throws IOException {
    int pathCount = pathsToDelete.size();
    if (pathCount == 0) {
      LOG.debug("No paths to delete");
      return 0;
    }
    int deleteLimit = getBulkDeleteLimit();
    Preconditions.checkArgument(pathCount > deleteLimit,
        "Too many files to delete (%d) limit is (%d)",
        deleteLimit, pathCount);

    if (deleteLimit == 1) {
      // the limit size will be 1 here, we know its not an empty list, so
      // go with it
      // and we know that this already blocks root delete operations.
      owner.delete(pathsToDelete.get(0), false);
      return 1;
    } else {
      List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>(pathCount);
      Map<String, Path> pathMap = new HashMap<>(pathCount);
      // this is a tree which is built up for mkdirs.
      // it is only for directories
      // root path is null.
      PathTreeEntry pathTree = new PathTreeEntry(new Path("/"));
      for (Path path : pathsToDelete) {
        String k = owner.pathToKey(path);
        owner.blockRootDelete(k);
        if (null != pathMap.put(k, path)) {
          keys.add(new DeleteObjectsRequest.KeyVersion(k));
          List<String> pathElements =
              MagicCommitPaths.splitPathToElements(path.getParent());
          if (!pathElements.isEmpty()) {
            pathTree.addChild(pathElements.iterator(), path);
          }
        }
      }
      // remove the keys.
      // This does not rebuild any fake directories; these are handled next.
      try {
        owner.removeKeys(keys, true, false);
        maybeMkdirLeafNodes(pathTree);
      } catch (MultiObjectDeleteException e) {
        // sync S3Guard with the partial failure
        Path firstPath = null;
        List<MultiObjectDeleteException.DeleteError> errors = e.getErrors();
        for (MultiObjectDeleteException.DeleteError error : errors) {
          Path removed = pathMap.remove(error.getKey());
          if (firstPath == null) {
            firstPath = removed;
          }
        }
        // Now remove all entries in the path map which weren't in the error
        // list, and which therefore are valid
        for (Path path : pathMap.values()) {
          owner.getMetadataStore().delete(path);
        }
        throw translateException("delete",
            firstPath != null ? firstPath.toString() : "",
            e);
        // this could support explicit extraction
      } catch (AmazonClientException e) {
        throw translateException("delete", "", e);
      }
      // update S3Guard
      if (owner.hasMetadataStore()) {
        for (Path path : pathMap.values()) {
          owner.getMetadataStore().delete(path);
        }
      }
      return pathMap.size();
    }

  }

  /**
   * Reject any request to delete an object where the key is root.
   * This is just lifted from S3AFS, just isolated to re
   * @param key key to validate
   * @throws InvalidRequestException if the request was rejected due to
   * a mistaken attempt to delete the root directory.
   */
  private void blockRootDelete(String key) throws InvalidRequestException {
    if (key.isEmpty() || "/".equals(key)) {
      throw new InvalidRequestException("Bucket cannot be deleted");
    }
  }

  /**
   * A specific type for path trees
   */
  private static class PathTreeEntry {

    private Map<String, PathTreeEntry> children;

    private Path dir;

    public PathTreeEntry(final Path dir) {
      this.dir = dir;
      children = new HashMap<>();
    }

    private boolean isLeaf() {
      return children.isEmpty();
    }

    private void addChild(Iterator<String> elements, Path p) {
      Preconditions.checkArgument(elements.hasNext(),
          "empty elements for path %s", p);
      String name = elements.next();
      if (!elements.hasNext()) {
        if (!children.containsKey(name)) {
          // mew entry
          children.put(name, new PathTreeEntry(p));
        } else {
          // leaf but existing entry. No-op
        }
      } else {
        // not a leaf entry, so add an intermediate node
        PathTreeEntry entry = children.get(name);
        if (entry == null) {
          // new entry
          entry = new PathTreeEntry(p);
          children.put(name, entry);
        }
        // at this point we have the path tree entry for the child elt
        // we are also confident that the iterator has an entry
        // so recurse down
        entry.addChild(elements, p);
      }
    }

    /**
     * Recursive listing of all leaf nodes.
     * @param leaves list to add entries to
     */
    private void enumLeafNodes(List<Path> leaves) {
      if (isLeaf()) {
        leaves.add(dir);
      } else {
        for (PathTreeEntry pathTreeEntry : children.values()) {
          pathTreeEntry.enumLeafNodes(leaves);
        }
      }
    }
  }
}
