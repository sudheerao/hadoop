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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import static org.apache.hadoop.fs.s3a.S3ABulkOperations.*;


/**
 * Unit tests for {@link S3ABulkOperations}, specifically
 * the underlying data structures.
 */
public class TestS3ABulkOperations extends Assert {

  static final Path R = new Path("s3a://bucket/");

  private final Path a = p(R, "a");
  private final Path b = p(R, "b");
  private final Path c = p(R, "c");

  private final Path ad = p(a, "d");
  private final Path adx = p(ad, "x");
  private final Path ady = p(ad, "y");
  private final Path ae = p(a, "e");
  private final Path af = p(a, "f");


  private Path p(Path p1, String elt) {
    return new Path(p1, elt);
  }

  private Iterator<String> seq(Path p) {
    return splitPathToElements(p).iterator();
  }

  private PathTree root;

  @Before
  public void setup() throws Exception {

    root = new PathTree(new Path("/"));
  }

  @Test
  public void testAssertionLogic() throws Throwable {
    assertNotEquals(R, a);
    verifyNotRoot(a);
    verifyNotRoot(ad);
    assertNotEquals(ad, this.a);
  }

  public Path verifyNotRoot(final Path p) {
    assertFalse("Root path " + p, p.isRoot());
    return p;
  }


  @Test
  public void testPathInsert() {
    assertAdded(seq(a), a);
  }

  private void assertAdded(Iterator<String> elements, Path p) {
    assertTrue("Did not add " + p,
        root.addChild(elements, p));
  }

  private void assertNotAdded(Iterator<String> elements, Path p) {
    assertFalse("Added " + p, root.addChild(elements, p));
  }


  private void assertAdded(Path p) {
    assertAdded(splitPathToElements(p).iterator(), p);
  }

  private void assertAdded(Path...paths) {
    for (Path path : paths) {
      assertAdded(path);
    }
  }

  private void assertNotAdded(Path p) {
    assertNotAdded(splitPathToElements(p).iterator(), p);
  }


  private PathTree childOf(
      PathTree entry, String key) {
    return entry.getChildren().get(key);
  }

  private PathTree assertChildOf(
      PathTree entry, String key) {
    PathTree child = childOf(entry, key);
    assertNotNull("Child not found " + key, child);
    return child;
  }

  private Set<Path> leaves(
      PathTree tree) {
    HashSet<Path> leaves = new HashSet<>();
    tree.leaves(leaves);
    return leaves;
  }

  /**
   * Verify that the leaves of a tree match the expected list.
   * @param t tree
   * @param paths possibly empty varargs list of paths
   * @return the leaves as a set.
   */
  private Set<Path> verifyLeavesEqual(
      PathTree t,
      Path... paths) {
    Set<Path> pathSet = new HashSet<Path>(Arrays.asList(paths));
    Set<Path> leaves = leaves(t);
    assertEquals(pathSet, leaves);
    return leaves;
  }

  @Test
  public void testRootIsNotALeaf() throws Throwable {
    assertNoLeaves();
  }

  /**
   * Double checks that the leaf set is the empty set.
   */
  public void assertNoLeaves() {
    assertTrue(verifyLeavesEqual(root).isEmpty());
  }

  @Test
  public void testDuplicates() throws Throwable {
    assertAdded(seq(a), a);
    assertChildOf(root, "a");
    assertNotAdded(seq(a), a);
  }

  /**
   * Add a parent, then a child.
   * @throws Throwable
   */
  @Test
  public void testChildUnderParent() throws Throwable {
    assertAdded(seq(a), a);
    assertAdded(seq(ad), ad);
    assertChildOf(assertChildOf(root, "a"), "d");
    verifyLeavesEqual(root, ad);
  }

  /**
   * Add a parent, then a child.
   * @throws Throwable
   */
  @Test
  public void testDuplicateChildren() throws Throwable {
    assertAdded(seq(ad), ad);
    assertNotAdded(seq(ad), ad);
    assertNotAdded(ad);
    verifyLeavesEqual(root, ad);
  }

  /**
   * Add a child node, then a parent; parent isn't added.
   * @throws Throwable
   */
  @Test
  public void testParentOverChild() throws Throwable {
    assertAdded(seq(ad), ad);
    assertNotAdded(seq(a), a);
    assertChildOf(assertChildOf(root, "a"), "d");
  }

  @Test
  public void testWideTree() throws Throwable {
    assertAdded(a, b, c, ad, ae, af);
    verifyLeavesEqual(root, af, ad, c, b, ae);
  }

  @Test
  public void testWideDeepTree() throws Throwable {
    assertAdded(a, b, c, adx, ady, ae, af);
    assertNotAdded(ad);
    assertNotAdded(a);
    verifyLeavesEqual(root, af, adx, ady, c, b, ae);
  }

  /**
   * Adding the root entry is not permitted with the root sequence
   */
  @Test(expected = IllegalArgumentException.class)
  public void testAddRoot() throws Throwable {
    root.addChild(seq(R), R);
  }

  /**
   * Abuse the class by adding the root entry under a path
   */
  @Test(expected = IllegalArgumentException.class)
  public void testAddRootWithKey() throws Throwable {
    // this is added as there are no precondition checks o
    root.addChild(seq(a), R);
  }

  /**
   * Relative paths aren't allowed.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRejectRelativePathsTree() throws Throwable {
    root.addChild(seq(a), new Path("b"));
  }

  /**
   * the iterator passed in must not be empty
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRejectEmptyIterator() throws Throwable {
    root.addChild(new ArrayList<String>().iterator(),
        new Path("b"));
  }

  /**
   * the iterator passed in must not be empty
   */
  @Test(expected = IllegalArgumentException.class)
  public void testRejectEmptyStringIterator() throws Throwable {
    root.addChild(Lists.newArrayList("").iterator(),
        new Path("b"));
  }

}
