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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.MagicCommitPaths;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.store.BulkIO;

/**
 * This performs the bulk IO so that it is isolated for testing.
 */
class S3ABulkOperations implements BulkIO {

  private static final Logger LOG = S3AFileSystem.LOG;

  private final S3AFileSystem owner;
  private final int pageSize;

  public S3ABulkOperations(final S3AFileSystem owner, final int pageSize) {
    this.owner = owner;
    this.pageSize = pageSize;
  }

  @Override
  public int getBulkDeleteFilesLimit() {
    return pageSize;
  }

  @Retries.RetryTranslated
  private int maybeMkdirLeafNodes(PathTree pathTreeEntry)
      throws IOException {
    List<Path> paths = new ArrayList<>(getBulkDeleteFilesLimit());
    pathTreeEntry.leaves(paths);
    LOG.info("Found {} directories to consider creating", paths.size());
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
  public int bulkDeleteFiles(final List<Path> filesToDelete) throws IOException {
    int pathCount = filesToDelete.size();
    if (pathCount == 0) {
      LOG.debug("No paths to delete");
      return 0;
    }
    int deleteLimit = getBulkDeleteFilesLimit();
    Preconditions.checkArgument(pathCount <= deleteLimit,
        "Too many files to delete (%s) limit is (%s)",
        deleteLimit, pathCount);
    int deleteCount;

    if (deleteLimit == 1 || pathCount == 1) {
      // optimized path for a single entry, either because that's the limit
      // or there is just one file
      owner.delete(filesToDelete.get(0), false);
      deleteCount = 1;
    } else {
      List<DeleteObjectsRequest.KeyVersion> deleteRequest =
          new ArrayList<>(pathCount);
      Map<String, Path> pathMap = new HashMap<>(pathCount);
      PathTree pathTree = prepareFilesForDeletion(filesToDelete,
          deleteRequest,
          pathMap);
      deleteCount = deleteRequest.size();

      // delete the keys.
      // This does not rebuild any fake directories; these are handled next.
      Invoker.once("Bulk delete", "",
          () -> {
            LOG.info("Deleting {} objects", deleteRequest.size());
            owner.removeKeys(deleteRequest, true, false);
            MetadataStore metadataStore = owner.getMetadataStore();
            if (metadataStore != null) {
              LOG.info("Deleting metastore references", deleteRequest.size());
              for (Path path : pathMap.values()) {
                metadataStore.delete(path);
              }
            }
            maybeMkdirLeafNodes(pathTree);
          });
    }
    return deleteCount;
  }

  /**
   * Prepare the files for deletion by building the datastructures
   * needed for the request and afterwards.
   * @param filesToDelete [in]: list of files
   * @param deleteRequest [out]: list of the keys of all paths to be used in
   * building the S3 API delete request.
   * @param pathMap map of keys to path for later use.
   * @return the tree of paths needed to identify directories to
   * maybe add mock markers to.
   */
  @VisibleForTesting
  PathTree prepareFilesForDeletion(final List<Path> filesToDelete,
      final List<DeleteObjectsRequest.KeyVersion> deleteRequest,
      final Map<String, Path> pathMap) {
    // this is a tree which is built up for mkdirs.
    // it is only for directories
    // root path is null.
    PathTree pathTree = new PathTree(new Path("/"));
    for (Path path : filesToDelete) {
      Preconditions.checkArgument(path.isAbsolute(),
          "Path %s is not absolute", path);
      String key = owner.pathToKey(path);
      Preconditions.checkArgument(!isRootKey(key),
          "Cannot delete the root path");
      if (null == pathMap.put(key, path)) {
        // not in the path map; so add it to the list of entries
        // in the delete request.
        LOG.debug("Adding {} to delete request", key);
        deleteRequest.add(new DeleteObjectsRequest.KeyVersion(key));
        Path parent = path.getParent();
        List<String> pathElements = splitPathToElements(parent);
        if (!pathElements.isEmpty()) {
          pathTree.addChild(pathElements.iterator(), parent);
        }
      }
    }
    return pathTree;
  }

  private boolean isRootKey(final String k) {
    return k.isEmpty() || "/".equals(k);
  }

  /**
   * Split a path to elements.
   * This references MagicCommitter code; its isolated so that
   * any backport to branch-2 only needs to inline one method.
   * @param path path
   * @return a list of elements within it.
   */
  @VisibleForTesting
  public static List<String> splitPathToElements(Path path) {
    return MagicCommitPaths.splitPathToElements(path);
  }

  /**
   * A specific type for path trees.
   * This is the tree built up to optimize parent directory creation after
   * the delete operation.
   * Only those directory paths which don't have any children need to go
   * through the {@code createFakeDirectoryIfNecessary} process.
   * As any directory in the operation which also has a child entry is
   * guaranteed to not need creation, they can be omitted.
   * All that is needed is to determine the lowest entries in the hierarchy,
   * which is done by:
   * <ol>
   *   <li>Split each path up into elements.</li>
   *   <li>Add to a tree using each element as the name of the node.</li>
   * </ol>
   * Later, {@link #enumLeafNodes(List)} can be used to enumerate all leaf
   * nodes, which can then be created.
   *
   * It is a requirement that all leaf nodes must have a path field, but
   * non-leaf nodes do not need to.
   *
   * Cost of operation.
   * <ul>
   *   <li>Insertion: O(depth) + cost of scanning/inserting child nodes</li>
   *   <li>Enumeration: O(nodes)</li>
   * </ul>
   * A simple hash map is used to store the children, so there's the cost
   * of scanning/expanding that if there are many child entries.
   * There's also the memory cost of all those hash tables.
   * The datastructure isn't "free", but "if it saves just one HTTPS call"
   * it's justified.
   */
  @VisibleForTesting
  public static class PathTree {

    /** Children hashmap */
    private final Map<String, PathTree> children;

    /**
     * Path of entry; will be null if the entry was added to the tree
     * when it was already known that this was a root node.
     */
    private final Path path;

    /**
     * Create an entry with the given directory.
     * @param path directory.
     */
    public PathTree(final Path path) {
      this.path = path;
      children = new HashMap<>();
    }

    public Map<String, PathTree> getChildren() {
      return children;
    }

    public Path getPath() {
      return path;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("PathTree{");
      sb.append("path=").append(path);
      sb.append("children#=").append(children.size());
      sb.append('}');
      return sb.toString();
    }

    /**
     * Is the entry a leaf node.
     * That is: it has no children.
     * @return true if the node is a leaf.
     */
    public boolean isLeaf() {
      return children.isEmpty();
    }

    /**
     * Recursively add a child.
     * @param elements list of remaining elements.
     * @param child path to add
     * @return true iff a child was added.
     */
    public boolean addChild(Iterator<String> elements, Path child) {
      Preconditions.checkArgument(elements.hasNext(),
          "Empty elements for path %s", child);
      String name = elements.next();
      Preconditions.checkArgument(!child.isRoot(),
          "Root path cannot be added for key %s: %s",
          name, path);
      Preconditions.checkArgument(child.isAbsolute(),
          "Non-absolute path for key %s: %s", name, path);
      boolean inserted;
      if (!elements.hasNext()) {
        if (!children.containsKey(name)) {
          // mew entry
          children.put(name, new PathTree(child));
          inserted = true;
        } else {
          // leaf but existing entry. No-op
          inserted = false;
        }
      } else {
        // not a leaf entry, so add an intermediate node
        PathTree entry = children.get(name);
        if (entry == null) {
          // new entry but not a leaf entry. Don't calculate a path.
          entry = new PathTree(null);
          children.put(name, entry);
        }
        // at this point we have the path tree entry for the child element
        // we are also confident that the iterator has an entry
        // so recurse down
        inserted = entry.addChild(elements, child);
      }
      return inserted;
    }

    /**
     * Recursive listing of all leaf nodes.
     * @param leaves list to add entries to
     */
    public void leaves(Collection<Path> leaves) {
      if (isLeaf()) {
        // the root of the tree won't have a path, so don't add it.
        if (path != null && !path.isRoot()) {
          leaves.add(path);
        }
      } else {
        for (PathTree pathTreeEntry : children.values()) {
          pathTreeEntry.leaves(leaves);
        }
      }
    }
  }
}
