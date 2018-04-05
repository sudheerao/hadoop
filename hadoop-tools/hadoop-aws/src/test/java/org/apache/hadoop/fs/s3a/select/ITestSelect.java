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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.Statistic;

import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;
import static org.apache.hadoop.fs.s3a.select.SelectBinding.*;

import static org.hamcrest.CoreMatchers.*;

/**
 * Test the S3 Select feature with some basic SQL Commands.
 * Executed if the destination store declares its support for the feature.
 */
public class ITestSelect extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestSelect.class);

  protected static final String TRUE = q("TRUE");
  protected static final String FALSE = q("FALSE");

  protected static final String ENTRY_0001 = "\"entry-0001\"";

  protected static final String ENTRY_0002 = "\"entry-0002\"";

  private static long ALL_QUOTES = 0x7fffffff;

  private static long NO_QUOTES = 0;

  private static int ALL_ROWS = 10;

  protected static final int ROWS_WITH_HEADER = ALL_ROWS + 1;

  private static final int ODD_ROWS = ALL_ROWS / 2;

  private Path csvPath;

  private Configuration selectConf;


  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue("S3 Select is not enabled",
        getFileSystem().hasCapability(S3_SELECT_CAPABILITY));
    csvPath = path(getMethodName() + ".csv");
    selectConf = new Configuration(getFileSystem().getConf());
    selectConf.set(HEADER, HEADER_OPT_USE);
  }


  @Override
  public void teardown() throws Exception {
    describe("teardown");
    try {
      if (csvPath != null) {
        getFileSystem().delete(csvPath, false);
      }
    } finally {
      super.teardown();
    }
  }

  /**
   * Base CSV file is headers
   * <pre>
   * "ID": index of the row
   * "date": date as Date.toString
   * "timestamp": timestamp in seconds of epoch
   * "name", entry-$row
   * "odd", odd/even as boolean
   * "oddint", odd/even as int : 1 for odd, 0 for even
   * "oddrange": odd/even as 1 for odd, -1 for even
   * </pre>
   */
  public void createStandardCsvFile(
      final Path path,
      final boolean header,
      final long quoteHeaderPolicy,
      final long quoteRowPolicy,
      final int rows,
      final String separator,
      final String eol,
      final String quote) throws IOException {
    try (CsvFile csv = new CsvFile(getFileSystem(),
        path,
        true,
        separator,
        eol,
        quote)) {

      if (header) {
        csv.row(quoteHeaderPolicy,
            "id",
            "date",
            "timestamp",
            "name",
            "odd",
            "oddint",
            "oddrange");
      }
      long now = System.currentTimeMillis();
      Date date = new Date(now);
      // loop is at 1 for use in counters and flags
      for (int i = 1; i <= rows; i++) {
        boolean odd = (i & 1) == 1;
        csv.row(quoteRowPolicy,
            i,
            date,
            now / 1000,
            String.format("entry-%04d", i),
            odd? "TRUE": "FALSE",
            odd ? 1 : 0,
            odd ? 1 : -1
        );
      }
      csv.close();
    }
  }

  /**
   * Create "the standard" CSV file with the default row count.
   * @param quoteRowPolicy what the row quote policy is.
   * @throws IOException IO failure.
   */

  private void createStandardCsvFile(final long quoteRowPolicy)
      throws IOException {
    createStandardCsvFile(
        csvPath,
        true,
        ALL_QUOTES,
        quoteRowPolicy,
        ALL_ROWS,
        ",",
        "\n",
        "\"");
  }

  @Test
  public void testCapabilityProbe() throws Throwable {

    // this is always true if we get past setup; its here to
    // provide a tested snipped of documentation.
    assertTrue(isSelectAvailable(getFileSystem()));
  }

  boolean isSelectAvailable(final FileSystem filesystem) {
    return filesystem instanceof StreamCapabilities
        && ((StreamCapabilities) filesystem).hasCapability("s3a:s3-select");
  }

  @Test
  public void testReadWholeFileClassicAPI() throws Throwable {
    describe("create and read the whole file. Verifies setup working");

    createStandardCsvFile(ALL_QUOTES);
    int lines;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        getFileSystem().open(csvPath)))) {
      lines = 0;
      String line;
      while ((line = reader.readLine()) != null) {
        lines++;
        LOG.info("{}", line);
      }
    }
    assertEquals("line count", ROWS_WITH_HEADER, lines);
  }

  @Test
  public void testSelectWholeFileNoHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    createStandardCsvFile(ALL_QUOTES);
    expectSelected(
        ALL_ROWS,
        selectConf,
        "SELECT * FROM S3OBJECT");
  }

  @Test
  public void testSelectFirstColumnNoHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    createStandardCsvFile(ALL_QUOTES);
    selectConf.set(HEADER, HEADER_OPT_NONE);
    expectSelected(
        ROWS_WITH_HEADER,
        selectConf,
        "SELECT s._1 FROM S3OBJECT s");
  }

  @Test
  public void testSelectSelfNoHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    createStandardCsvFile(ALL_QUOTES);
    selectConf.set(HEADER, HEADER_OPT_NONE);
    expectSelected(
        ROWS_WITH_HEADER,
        selectConf,
        "SELECT s._1 FROM S3OBJECT s WHERE s._1 = s._1");
  }


  @Test
  public void testSelectSelfUseHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    createStandardCsvFile(ALL_QUOTES);
    selectConf.set(HEADER, HEADER_OPT_USE);
    expectSelected(
        ALL_ROWS,
        selectConf,
        "SELECT s.id FROM S3OBJECT s WHERE s.id = s.id");
  }

  @Test
  public void testSelectID2UseHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    createStandardCsvFile(ALL_QUOTES);
    selectConf.set(HEADER, HEADER_OPT_USE);
    expectSelected(
        1,
        selectConf,
        "SELECT s.id FROM S3OBJECT s WHERE s.id = '2'");
  }

  @Test
  public void testSelectId1() throws Throwable {
    describe("Select the first element in the file");
    createStandardCsvFile(ALL_QUOTES);
    selectConf.set(HEADER, HEADER_OPT_NONE);
    expectSelected(
        1,
        selectConf,
        "SELECT * FROM S3OBJECT s WHERE s._1 = '1'",
        TRUE);
  }


  @Test
  public void testSelectOddLinesNoHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    createStandardCsvFile(ALL_QUOTES);
    selectConf.set(HEADER, HEADER_OPT_IGNORE);
    expectSelected(
        ODD_ROWS,
        selectConf,
        "SELECT * FROM S3OBJECT s WHERE %s = s._5",
        TRUE);
    // and do a quick check on the instrumentation
    long bytesRead = getFileSystem().getInstrumentation()
        .getCounterValue(Statistic.STREAM_SEEK_BYTES_READ);
    assertNotEquals("No bytes read count", 0, bytesRead);
  }

  @Test
  public void testSelectOddLinesHeader() throws Throwable {
    describe("Select the odd values");
    createStandardCsvFile(ALL_QUOTES);
    selectConf.set(HEADER, HEADER_OPT_USE);
    List<String> selected = expectSelected(
        ODD_ROWS,
        selectConf,
        "SELECT s.name FROM S3OBJECT s WHERE s.odd = %s",
        TRUE);
    // the list includes odd values
    assertThat(selected, hasItem(ENTRY_0001));
    // but not the evens
    assertThat(selected, not(hasItem(ENTRY_0002)));
  }

  @Test
  public void testSelectNotOperationHeader() throws Throwable {
    describe("Select the even values with a NOT call; quote the header name");
    createStandardCsvFile(ALL_QUOTES);
    selectConf.set(HEADER, HEADER_OPT_USE);
    List<String> selected = expectSelected(
        ODD_ROWS,
        selectConf,
        "SELECT s.name FROM S3OBJECT s WHERE NOT s.\"odd\" = %s",
        TRUE);
    // the list includes no odd values
    assertThat(selected, not(hasItem(ENTRY_0001)));
    // but has the evens
    assertThat(selected, hasItem(ENTRY_0002));
  }


  @Test
  public void testBackslashExpansion() throws Throwable {

    assertEquals("\t\r\n", expandBackslashChars("\t\r\n"));
    assertEquals("\t", expandBackslashChars("\\t"));
    assertEquals("\r", expandBackslashChars("\\r"));
    assertEquals("\r \n", expandBackslashChars("\\r \\n"));

  }

  /**
   * Build the SQL statement, using String.Format rules
   * @param template template
   * @param args arguments for the template
   * @return the template to use
   */
  private static String sql(
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

  /**
   * Issue a select call, expect the specific number of rows back.
   * Error text will include the SQL.
   * @param expected expected row count.
   * @param conf config for the select call.
   * @param template template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the lines selected
   * @throws IOException failure
   */
  private List<String> expectSelected(
      final int expected,
      final Configuration conf,
      final String template,
      final Object... args) throws IOException {
    List<String> selection = selectCsvFile(conf, template, args);
    assertEquals("row count from select call " + sql(template, args),
        expected,
        selection.size());
    return selection;
  }

  /**
   * Select from the CSV file.
   * @param conf config for the select call.
   * @param template template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the lines selected
   * @throws IOException failure
   */
  private List<String> selectCsvFile(
      final Configuration conf,
      final String template,
      final Object... args) throws IOException {
    List<String> result = new ArrayList<>();
    String expression = sql(template, args);
    describe("Execution Select call: %s", expression);
    FSDataInputStream select = getFileSystem()
        .select(csvPath,
            expression,
            conf);
    try (Scanner scanner = new Scanner(
             new BufferedReader(new InputStreamReader(select)))) {
      scanner.useDelimiter("\n");
      while (scanner.hasNextLine()) {
        String l = scanner.nextLine();
        LOG.info("{}", l);
        result.add(l);
      }
    }
    describe("Result line count: %s\nStatistics\n%s",
        result.size(), select);
    return result;
  }

}
