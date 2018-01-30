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

package org.apache.hadoop.fs.s3a.scale;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3ABulkOperations;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ABulkOperations.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * S3A Integration tests for bulk upload.
 * There's a special shortcut for a single file operation, so tests
 * need to check coverage on both paths by doing single and double operations.
 */
public class ITestS3ABulkOperations extends S3AScaleTestBase {

  private static final int DELETE_WAIT_TIMEOUT = 10000;

  private static final int DELETE_WAIT_INTERVAL = 1000;

  private static final String ROOT_ERROR_TEXT = "root";

  /**
   * Directory creation constants.
   */
  private final int levels = 3;
  private final int dirsPerLevel = 2;
  private final int filesPerLevel = 3;

  private final int leaves =
      (int)(Math.pow(dirsPerLevel, levels) * filesPerLevel);

  /**
   * The number of immediate parent directories we expect to see created.
   */
  private final int immediateParents =
      (int)Math.pow(dirsPerLevel, levels - 1 );


  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ABulkOperations.class);

  /**
   * Create a tree of files.
   * @param base base path for this entry
   * @param depth depth remaining
   * @param dirs dirs per level
   * @param files files per dir
   * @param dirsCreated out: counter of directories
   * @return the paths of the created files.
   * @throws IOException
   */
  private List<Path> createFileTree(Path base,
      int depth,
      int dirs,
      int files,
      AtomicInteger dirsCreated) throws IOException {
    List<Path> paths = new ArrayList<>(leaves);
    dirsCreated.set(buildFilenames(paths, base, depth, dirs, files));
    Collections.shuffle(paths);
    S3AFileSystem fs = getFileSystem();
    for (Path path : paths) {
      touch(path);
    }
    return paths;
  }

  /**
   * Recursive path builder.
   * @param paths list to build up
   * @param base base path for this entry
   * @param depth depth remaining
   * @param dirs dirs per level
   * @param files files per dir
   * @return the number of directories implicitly created.
   */

  private int buildFilenames(Collection<Path> paths,
      Path base,
      int depth,
      int dirs,
      int files) {
    if (depth == 0) {
      return 0;
    }
    for (int i = 1; i <= files; i++) {
      paths.add(new Path(base, String.format("file-%04d", i)));
    }
    int dirsCreated = 0;
    for (int i = 1; i <= dirs; i++) {
      int nextDepth = depth - 1;
      dirsCreated = 1 + buildFilenames(paths,
          new Path(base, String.format("dir-%04d", i)),
          nextDepth,
          dirs,
          files);
    }
    return dirsCreated;
  }

  @Test
  public void testCreateAndDelete() throws Exception {
    S3AFileSystem fs = getFileSystem();
    Path base = path(getMethodName());
    NanoTimer creation = new NanoTimer();
    AtomicInteger dirsToCreate = new AtomicInteger();
    List<Path> files = createFileTree(base, levels, dirsPerLevel,
        filesPerLevel, dirsToCreate);
    int fileCount = files.size();
    creation.end("Creation of %d files under %s", fileCount, base);
    LOG.info("Time per creation: {} nS",
        creation.nanosPerOperation(fileCount));
    for (Path file : files) {
      LOG.info("{}", file);
    }
    S3ATestUtils.MetricDiff deleteRequests =
        new S3ATestUtils.MetricDiff(fs, Statistic.OBJECT_DELETE_REQUESTS);
    S3ATestUtils.MetricDiff directoriesDeleted =
        new S3ATestUtils.MetricDiff(fs, Statistic.DIRECTORIES_DELETED);
    S3ATestUtils.MetricDiff fakeDirectoriesDeleted =
        new S3ATestUtils.MetricDiff(fs, Statistic.FAKE_DIRECTORIES_DELETED);
    S3ATestUtils.MetricDiff directoriesCreated =
        new S3ATestUtils.MetricDiff(fs, Statistic.DIRECTORIES_CREATED);
    NanoTimer deletion = new NanoTimer();
    int deleted = fs.bulkDeleteFiles(files);
    deletion.end("deleted of %d files under %s", deleted, base);
    LOG.info("Time per deletion: {} nS",
        deletion.nanosPerOperation(fileCount));
    LOG.info("filesystem Stats\n{}", fs);

    assertEquals(fileCount, deleted);

    int directoriesToCreate = immediateParents;
    directoriesCreated.assertDiffEquals(immediateParents);
    directoriesDeleted.assertDiffEquals(0);
    deleteRequests.assertDiffEquals(1 + directoriesToCreate);

    LOG.info("Directory listing");
    S3ATestUtils.lsR(fs, base, true);

    // allow time for this to stabilize
    eventually(DELETE_WAIT_TIMEOUT, DELETE_WAIT_INTERVAL,
        () -> {
          for (Path file : files) {
            assertPathDoesNotExist("expected deleted", file);
            assertIsDirectory(file.getParent());
          }
        });
  }

  @Test
  public void testEmptyDelete() throws Exception {
    assertEquals(0,
        getFileSystem().bulkDeleteFiles(new ArrayList<>(0)));
  }

  @Test
  public void testDeleteOneEntryPath() throws Throwable {
    Path p = touch(path(getMethodName()));
    expectBulkDelete(1, p);
    assertPathDoesNotExist("expected deleted", p);
  }

  @Test
  public void testDeleteTwoEntryPath() throws Throwable {
    Path parent = path(getMethodName());

    S3AFileSystem fs = getFileSystem();
    Path child1 = touch(new Path(parent, "child1"));
    Path child2 = touch(new Path(parent, "child2"));
    expectBulkDelete(2, child1, child2);
    assertPathsDoNotExist(fs, "expected deleted", child1, child2);
    assertIsDirectory(parent);
  }

  @Test
  public void testDeleteMissingPath() throws Throwable {
    describe("Deleting a nonexistent file isn't discovered");
    Path p = path(getMethodName());
    expectBulkDelete(1, p);
    assertPathDoesNotExist("expected deleted", p);
    assertIsDirectory(p.getParent());
  }

  @Test
  public void testDeleteMultipleMissingPaths() throws Throwable {
    describe("Delete multiple missing paths; expect the parent directory"
        + " to be created -there are no probes for the paths existing.");
    Path parent = path(getMethodName());
    expectBulkDelete(2,
        new Path(parent, "child1"),
        new Path(parent, "child2"));
    assertIsDirectory(parent);
  }

  @Test
  public void testDeleteDuplicatePaths() throws Throwable {
    describe("Delete duplicate paths");
    Path parent = path(getMethodName());
    expectBulkDelete(1,
        new Path(parent, "child1"),
        new Path(parent, "child1"));
    assertIsDirectory(parent);
  }

  @Test
  public void testDeleteEmptyDirectory() throws Throwable {
    describe("Expect a directory marker deletion to be ignored");
    Path dir = path(getMethodName());
    mkdirs(dir);
    expectBulkDelete(1, dir);
    assertIsDirectory(dir);
  }
  @Test
  public void testDeleteNonEmptyDirectory() throws Throwable {
    describe("Expect a directory marker deletion to be ignored");
    Path dir = path(getMethodName());
    Path child = touch(new Path(dir, "child"));
    expectBulkDelete(1, dir);
    assertIsFile(child);
    assertIsDirectory(dir);
  }

  @Test
  public void testDeleteRelativePath() throws Throwable {
    bulkDelete(new Path("relative"));
  }

  @Test
  public void testDeleteRelativePath2() throws Throwable {
    interceptBulkDelete(PathIOException.class,
        S3ABulkOperations.ERR_PATH_NOT_ABSOLUTE,
        new Path("relative"), new Path("../somewhere"));
  }

  @Test
  public void testDeleteRootPath() throws Throwable {
    interceptBulkDelete(PathIOException.class, ROOT_ERROR_TEXT,
        new Path("/"));
  }

  @Test
  public void testDeleteRootPath2() throws Throwable {
    interceptBulkDelete(PathIOException.class, ROOT_ERROR_TEXT,
        new Path("/"), path(getMethodName()));
  }

  @Test
  public void testDeletePastPageSize() throws Throwable {
    describe("Create a bulk operations with a small page and test");
    S3ABulkOperations bulkOperations = new S3ABulkOperations(
        getFileSystem(), 3);
    assertEquals(3, bulkOperations.getBulkDeleteFilesLimit());
    Path p = path(getMethodName());
    intercept(IllegalArgumentException.class,
        ERR_TOO_MANY_FILES_TO_DELETE,
        () ->
            bulkOperations.bulkDeleteFiles(
                Lists.newArrayList(p, p, p, p, p)));
  }

  @Test
  public void testDeleteDisabled() throws Throwable {
    describe("Create a bulk operations with bulk deletes disabled and test");
    S3ABulkOperations bulkOperations = new S3ABulkOperations(
        getFileSystem(), 0);
    assertEquals(0, bulkOperations.getBulkDeleteFilesLimit());
    intercept(IllegalArgumentException.class,
        ERR_BULK_DELETE_DISABLED,
        () ->
            bulkOperations.bulkDeleteFiles(
                Lists.newArrayList(path(getMethodName()))));
  }


  /**
   * Touch a file and return the path.
   * @param p path
   * @return the path passed in
   * @throws IOException IO Failure
   */
  private Path touch(Path p) throws IOException {
    ContractTestUtils.touch(getFileSystem(), p);
    return p;
  }

  /**
   * Bulk delete the list of paths.
   * @param paths paths to delete
   * @return the count of actual deletions
   * @throws IOException failure
   */
  private int bulkDelete(Path...paths) throws IOException {
    return getFileSystem().bulkDeleteFiles(Lists.newArrayList(paths));
  }

  /**
   * Expect a bulk delete operation to delete a specific number of entries,
   * after duplicates were resolved.
   * @param count expected count
   * @param paths list of paths
   * @throws IOException IO Failure
   */
  private void expectBulkDelete(int count, Path...paths) throws IOException {
    assertEquals("deletion count", count, bulkDelete(paths));
  }

  /**
   * Invoke Bulk Delete and expect an exception.
   * @param clazz class type expected
   * @param text text in exception
   * @param paths list of paths
   * @param <E> return type
   * @return the caught exception
   * @throws Exception any other exception raised.
   */
  private <E extends Throwable> E interceptBulkDelete(Class<E> clazz,
      String text, Path...paths) throws Exception {
    return intercept(clazz, text,
        () -> bulkDelete(paths));
  }

}
