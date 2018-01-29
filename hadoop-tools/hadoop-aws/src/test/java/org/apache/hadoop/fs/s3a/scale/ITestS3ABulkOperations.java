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
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.test.LambdaTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
/**
 * S3A tests for configuring block size.
 */
public class ITestS3ABulkOperations extends S3AScaleTestBase {

  /**
   * Directory creation
   */
  private final int levels = 2;
  private final int dirsPerLevel = 2;
  private final int filesPerLevel = 3;

  private final int leaves = (int)(Math.pow(dirsPerLevel, levels) * filesPerLevel);

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ABulkOperations.class);

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
      touch(fs, path);
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
      dirsCreated = 1 + buildFilenames(paths,
          new Path(base, String.format("dir-%04d", i)),
          depth - 1,
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


    int directoriesToCreate = dirsToCreate.get();
    directoriesCreated.assertDiffEquals(directoriesToCreate);
    directoriesDeleted.assertDiffEquals(0);
    deleteRequests.assertDiffEquals(1 + directoriesToCreate);

    LOG.info("Directory listing");
    S3ATestUtils.lsR(fs, base, true);


    // allow up to 10s for this to stabilize
    LambdaTestUtils.eventually(10000, 1000,
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
    Path p = path(getMethodName());
    S3AFileSystem fs = getFileSystem();
    ArrayList<Path> paths = Lists.newArrayList(p);
    touch(fs, p);
    int bulkDeleteCount = fs.bulkDeleteFiles(paths);
    assertEquals("deletion count", 1, bulkDeleteCount);
    assertPathDoesNotExist("expected deleted", p);
  }

  @Test
  public void testDeleteMissingPath() throws Throwable {
    describe("Deleting a nonexistent file isn't discovered");
    Path p = path(getMethodName());
    S3AFileSystem fs = getFileSystem();
    ArrayList<Path> paths = Lists.newArrayList(p);
    assertEquals("deletion count", 1, fs.bulkDeleteFiles(paths));
    assertPathDoesNotExist("expected deleted", p);
  }

  @Test
  public void testDeleteRelativePath() throws Throwable {

  }

  @Test
  public void testDeleteRootPath() throws Throwable {

  }


  @Test
  public void testDeletePathsWhichDontExist() throws Throwable {

  }


}
