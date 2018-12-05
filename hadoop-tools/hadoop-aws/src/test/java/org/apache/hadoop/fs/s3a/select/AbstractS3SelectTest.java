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

package org.apache.hadoop.fs.s3a.select;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

import org.junit.Assume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;

import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;

/**
 * Superclass for S3 Select tests.
 */
public abstract class AbstractS3SelectTest extends AbstractS3ATestBase {

  protected static final String TRUE = q("TRUE");

  protected static final String FALSE = q("FALSE");

  /**
   * Setup: requires select to be available.
   */
  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue("S3 Select is not enabled",
        getFileSystem().hasCapability(S3_SELECT_CAPABILITY));
  }

  /**
   * Build the SQL statement, using String.Format rules.
   * @param template template
   * @param args arguments for the template
   * @return the template to use
   */
  protected static String sql(
      final String template,
      final Object... args) {
    return args.length > 0 ? String.format(template, args) : template;
  }

  /**
   * Quote a constant with the SQL quote logic.
   * @param c constant
   * @return quoted constant
   */
  private static String q(String c) {
    return '\'' + c + '\'';
  }

  boolean isSelectAvailable(final FileSystem filesystem) {
    return filesystem instanceof StreamCapabilities
        && ((StreamCapabilities) filesystem).hasCapability(
        SelectConstants.S3_SELECT_CAPABILITY);
  }

  /**
   * Select from a source file.
   * @param fileSystem FS.
   * @param source source file.
   * @param conf config for the select call.
   * @param sql template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the input stream.
   * @throws IOException failure
   */
  protected FSDataInputStream select(
      final FileSystem fileSystem,
      final Path source,
      final Configuration conf,
      final String sql,
      final Object... args)
      throws IOException, ExecutionException, InterruptedException {
    String expression = sql(sql, args);
    describe("Execution Select call: %s", expression);
    FutureDataInputStreamBuilder builder =
        fileSystem.openFile(source)
            .must(S3A_SELECT_SQL, expression);
    // propagate all known options
    for (String key : SELECT_OPTIONS) {
      String value = conf.get(key);
      if (value != null) {
        builder.must(key, value);
      }
    }
    return builder.build().get();
  }

  /**
   * Select from a source file via the file context API.
   * @param fc file context
   * @param source source file.
   * @param conf config for the select call.
   * @param sql template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the input stream.
   * @throws IOException failure
   */
  protected FSDataInputStream select(
      final FileContext fc,
      final Path source,
      final Configuration conf,
      final String sql,
      final Object... args)
      throws IOException, ExecutionException, InterruptedException {
    String expression = sql(sql, args);
    describe("Execution Select call: %s", expression);
    FutureDataInputStreamBuilder builder = fc.openFile(source)
        .must(S3A_SELECT_SQL, expression);
    // propagate all known options
    SELECT_OPTIONS.forEach((key) ->
        Optional.ofNullable(conf.get(key))
            .map((v) -> builder.must(key, v)));
    return builder.build().get();
  }

  /**
   * Parse a selection to lines; log at info.
   * @param selection selection input
   * @param maxLines maximum number of lines.
   * @return a list of lines.
   * @throws IOException if raised during the read.
   */
  protected List<String> parseToLines(final FSDataInputStream selection,
      int maxLines)
      throws IOException {
    List<String> result = new ArrayList<>();
    String stats;
    // the scanner assumes that any IOE => EOF; we don't want
    // that and so will check afterwards.
    try (Scanner scanner = new Scanner(
        new BufferedReader(new InputStreamReader(selection)))) {
      scanner.useDelimiter(CSV_INPUT_RECORD_DELIMITER_DEFAULT);
      while (maxLines > 0) {
        try {
          String l = scanner.nextLine();
          LOG.info("{}", l);
          result.add(l);
          maxLines--;
        } catch (NoSuchElementException e) {
          // EOL or an error
          break;
        }
      }
      stats = selection.toString();
      describe("Result line count: %s\nStatistics\n%s",
          result.size(), stats);
      // look for any raised error.
      IOException ioe = scanner.ioException();
      if (ioe != null && !(ioe instanceof EOFException)) {
        throw ioe;
      }
    }
    return result;
  }

  /**
   * Verify the selection count; return the original list.
   * If there's a mismatch, the whole list is logged at error, then
   * an assertion raised.
   * @param expected expected value.
   * @param expression expression -for error messages.
   * @param selection selected result.
   * @return the input list.
   */
  protected List<String> verifySelectionCount(
      final int expected,
      final String expression,
      final List<String> selection) {
    return verifySelectionCount(expected, expected, expression, selection);
  }

  /**
   * Verify the selection count; return the original list.
   * If there's a mismatch, the whole list is logged at error, then
   * an assertion raised.
   * @param min min value (exclusive).
   * @param max max value (exclusive). If -1: no maximum.
   * @param expression expression -for error messages.
   * @param selection selected result.
   * @return the input list.
   */
  protected List<String> verifySelectionCount(
      final int min,
      final int max,
      final String expression,
      final List<String> selection) {
    int size = selection.size();
    if (size < min || (max > -1 && size > max)) {
      // mismatch: log and then fail
      String listing = String.join("\n", selection);
      LOG.error("\n{} => \n{}", expression, listing);
      fail("row count from select call " + expression
          + " is out of range " + min + " to " + max
          + ": " + size
          + " \n" + listing);
    }
    return selection;
  }

}
