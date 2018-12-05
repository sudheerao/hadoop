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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;

import static org.apache.hadoop.fs.s3a.S3AUtils.s3a;
import static org.apache.hadoop.fs.s3a.select.CsvFile.ALL_QUOTES;
import static org.apache.hadoop.fs.s3a.select.SelectBinding.expandBackslashChars;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;

/**
 * Test the S3 Select feature with some basic SQL Commands.
 * Executed if the destination store declares its support for the feature.
 */
public class ITestS3Select extends AbstractS3SelectTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3Select.class);

  protected static final String ENTRY_0001 = "\"entry-0001\"";

  protected static final String ENTRY_0002 = "\"entry-0002\"";

  private static final int ALL_ROWS = 10;

  protected static final int ROWS_WITH_HEADER = ALL_ROWS + 1;

  private static final int ODD_ROWS = ALL_ROWS / 2;

  public static final String SELECT_ODD_ROWS
      = "SELECT s.name FROM S3OBJECT s WHERE s.odd = " + TRUE;

  public static final String SELECT_ODD_ENTRIES
      = "SELECT * FROM S3OBJECT s WHERE s.\"odd\" = `TRUE`";

  private Path csvPath;

  private Configuration selectConf;

  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue("S3 Select is not enabled",
        getFileSystem().hasCapability(S3_SELECT_CAPABILITY));
    csvPath = path(getMethodName() + ".csv");
    selectConf = new Configuration(false);
    createStandardCsvFile(ALL_QUOTES);
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
   * Base CSV file is headers.
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
            odd ? "TRUE" : "FALSE",
            odd ? 1 : 0,
            odd ? 1 : -1
        );
      }
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

    // this should always hold true if we get past test setup
    assertTrue("Select is not available on " + getFileSystem(),
        isSelectAvailable(getFileSystem()));
  }

  @Test
  public void testReadWholeFileClassicAPI() throws Throwable {
    describe("create and read the whole file. Verifies setup working");
    int lines;
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(
            getFileSystem().open(csvPath)))) {
      lines = 0;
      // seek to 0, which is what some input formats do
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
    expectSelected(
        ALL_ROWS,
        selectConf,
        HEADER_OPT_USE,
        "SELECT * FROM S3OBJECT");
  }

  @Test
  public void testSelectFirstColumnNoHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    expectSelected(
        ROWS_WITH_HEADER,
        selectConf,
        HEADER_OPT_NONE,
        "SELECT s._1 FROM S3OBJECT s");
  }

  @Test
  public void testSelectSelfNoHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    expectSelected(
        ROWS_WITH_HEADER,
        selectConf,
        HEADER_OPT_NONE,
        "SELECT s._1 FROM S3OBJECT s WHERE s._1 = s._1");
  }

  @Test
  public void testSelectSelfUseHeader() throws Throwable {
    describe("Select the entire file, expect all rows including the header");
    expectSelected(
        ALL_ROWS,
        selectConf,
        HEADER_OPT_USE,
        "SELECT s.id FROM S3OBJECT s WHERE s.id = s.id");
  }

  @Test
  public void testSelectID2UseHeader() throws Throwable {
    describe("Select where ID=2; use the header");
    expectSelected(
        1,
        selectConf,
        HEADER_OPT_USE,
        "SELECT s.id FROM S3OBJECT s WHERE s.id = '2'");
  }

  @Test
  public void testSelectNoMatchingID() throws Throwable {
    describe("Select where there is no match; expect nothing back");
    expectSelected(
        0,
        selectConf,
        HEADER_OPT_USE,
        "SELECT s.id FROM S3OBJECT s WHERE s.id = '0x8000'");
  }

  @Test
  public void testSelectId1() throws Throwable {
    describe("Select the first element in the file");
    expectSelected(
        1,
        selectConf,
        HEADER_OPT_NONE,
        "SELECT * FROM S3OBJECT s WHERE s._1 = '1'",
        TRUE);
  }

  @Test
  public void testSelectEmptySQL() throws Throwable {
    describe("An empty SQL statement fails fast");
    FutureDataInputStreamBuilder builder = getFileSystem().openFile(
        csvPath)
        .must(S3A_SELECT_SQL, "");
    interceptFuture(IllegalArgumentException.class,
        S3A_SELECT_SQL,
        builder.build());
  }

  @Test
  public void testSelectSeekRestricted() throws Throwable {
    describe("Verify that only seeks to the current pos work");
    try (FSDataInputStream stream =
             select(getFileSystem(), csvPath,
                 selectConf,
                 "SELECT * FROM S3OBJECT s WHERE s._1 = '1'")) {
      stream.seek(0);
      stream.read();
      stream.seek(1);
      stream.seek(1);
      intercept(PathIOException.class,
          () -> stream.seek(0));
      byte[] buffer = new byte[1];
      stream.readFully(stream.getPos(), buffer);
      intercept(PathIOException.class,
          () -> stream.readFully(1, buffer));
    }
  }

  @Test
  public void testSelectOddLinesNoHeader() throws Throwable {
    describe("Select odd lines, ignoring the header");
    expectSelected(
        ODD_ROWS,
        selectConf,
        HEADER_OPT_IGNORE,
        "SELECT * FROM S3OBJECT s WHERE s._5 = `TRUE`");
    // and do a quick check on the instrumentation
    long bytesRead = getFileSystem().getInstrumentation()
        .getCounterValue(Statistic.STREAM_SEEK_BYTES_READ);
    assertNotEquals("No bytes read count", 0, bytesRead);
  }

  @Test
  public void testSelectOddLinesHeader() throws Throwable {
    describe("Select the odd values");
    List<String> selected = expectSelected(
        ODD_ROWS,
        selectConf,
        HEADER_OPT_USE,
        SELECT_ODD_ROWS);
    // the list includes odd values
    assertThat(selected, hasItem(ENTRY_0001));
    // but not the evens
    assertThat(selected, not(hasItem(ENTRY_0002)));
  }

  @Test
  public void testSelectNotOperationHeader() throws Throwable {
    describe("Select the even values with a NOT call; quote the header name");
    List<String> selected = expectSelected(
        ODD_ROWS,
        selectConf,
        HEADER_OPT_USE,
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
    assertEquals("\\", expandBackslashChars("\\\\"));
  }

  /**
   * This is an expanded example for the documentation.
   * Also helps catch out unplanned changes to the configuration strings.
   */
  @Test
  public void testSelectFileExample() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    int len = (int) getFileSystem().getFileStatus(csvPath).getLen();
    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(csvPath)
            .must("s3a:select.sql",
                SELECT_ODD_ENTRIES)
            .must("s3a:fs.s3a.select.input.compression", "NONE")
            .must("s3a:fs.s3a.select.input.csv.header", "use");

    CompletableFuture<FSDataInputStream> future = builder.build();
    try (FSDataInputStream select = future.get()) {
      // process the output
      byte[] bytes = new byte[len];
      int actual = select.read(bytes);
      LOG.info("file length is {}; length of selected data is {}",
          len, actual);
    }
  }

  /**
   * This is an expanded example for the documentation.
   * Also helps catch out unplanned changes to the configuration strings.
   */
  @Test
  public void testSelectMissingFile() throws Throwable {

    describe("Select a missing file, expect it to surface in the future");

    Path missing = path("missing");

    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(missing)
            .must("s3a:select.sql",
                SELECT_ODD_ENTRIES);

    interceptFuture(FileNotFoundException.class,
        "", builder.build());
  }

  @Test
  public void testSelectDirectory() throws Throwable {
    describe("Verify that secondary select options are only valid on select"
        + " queries");
    S3AFileSystem fs = getFileSystem();
    Path dir = path("dir");
    // this will be an empty dir marker
    fs.mkdirs(dir);

    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(dir)
            .must("s3a:select.sql", SELECT_ODD_ENTRIES);
    interceptFuture(FileNotFoundException.class,
        "", builder.build());

    // try the parent
    builder = getFileSystem().openFile(dir.getParent())
            .must("s3a:select.sql",
                SELECT_ODD_ENTRIES);
    interceptFuture(FileNotFoundException.class,
        "", builder.build());
  }

  @Test
  public void testSelectRoot() throws Throwable {
    describe("verify root dir selection is rejected");
    S3AFileSystem fs = getFileSystem();
    Path dir = path("dir");
    // this will be an empty dir marker
    fs.mkdirs(dir);

    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(dir)
            .must("s3a:select.sql", SELECT_ODD_ENTRIES);
    interceptFuture(FileNotFoundException.class,
        "", builder.build());

    // try the parent
    builder = getFileSystem().openFile(dir.getParent())
            .must("s3a:select.sql", SELECT_ODD_ENTRIES);
    interceptFuture(FileNotFoundException.class,
        "", builder.build());
  }

  @Test
  public void testCloseWithAbort() throws Throwable {
    describe("Close the stream with the readahead outstandingV");
    FSDataInputStream stream = select(getFileSystem(), csvPath, selectConf,
        "SELECT * FROM S3OBJECT s");
    stream.setReadahead(1L);
    stream.read();
    SelectInputStream sis = (SelectInputStream) stream.getWrappedStream();
    S3AInstrumentation.InputStreamStatistics stats
        = sis.getS3AStreamStatistics();
    assertEquals("Read count in " + sis,
        1, stats.bytesRead);
    stream.close();
    assertEquals("Abort count in " + sis,
        1, stats.aborted);
  }

  @Test
  public void testCloseWithNoAbort() throws Throwable {
    describe("Close the stream with the readahead outstandingV");
    FSDataInputStream stream = select(getFileSystem(), csvPath, selectConf,
        "SELECT * FROM S3OBJECT s");
    stream.setReadahead(0x1000L);
    SelectInputStream sis = (SelectInputStream) stream.getWrappedStream();
    S3AInstrumentation.InputStreamStatistics stats
        = sis.getS3AStreamStatistics();
    stream.close();
    assertEquals("Close count in " + sis, 1, stats.closed);
    assertEquals("Abort count in " + sis, 0, stats.aborted);
    assertTrue("No bytes read in close of " + sis, stats.bytesReadInClose > 0);
  }

  @Test
  public void testFileContextIntegration() throws Throwable {
    describe("Test that select works through FileContext");
    FileContext fc = S3ATestUtils.createTestFileContext(getConfiguration());
    selectConf.set(s3a(CSV_INPUT_HEADER), HEADER_OPT_USE);

    List<String> selected =
        verifySelectionCount(ODD_ROWS, SELECT_ODD_ROWS,
            parseToLines(
                select(fc, csvPath, selectConf, SELECT_ODD_ROWS),
                1000));
    // the list includes odd values
    assertThat(selected, hasItem(ENTRY_0001));
    // but not the evens
    assertThat(selected, not(hasItem(ENTRY_0002)));
  }

  @Test
  public void testSelectOptionsOnlyOnSelectCalls() throws Throwable {
    describe("Verify that secondary select options are only valid on select"
        + " queries");
    String key = s3a(CSV_INPUT_HEADER);
    intercept(IllegalArgumentException.class, key,
        () -> getFileSystem().openFile(csvPath)
            .must(key, HEADER_OPT_USE).build());
  }

  @Test
  public void testSelectMustBeEnabled() throws Throwable {
    describe("Verify that the FS must have S3 select enabled.");
    Configuration conf = new Configuration(getFileSystem().getConf());
    conf.setBoolean(FS_S3A_SELECT_ENABLED, false);
    try (FileSystem fs2 = FileSystem.newInstance(csvPath.toUri(), conf)) {
      intercept(PathIOException.class,
          SELECT_UNSUPPORTED,
          () -> fs2.openFile(csvPath)
              .must(S3A_SELECT_SQL, SELECT_ODD_ROWS)
              .build());
    }
  }

  /**
   * Issue a select call, expect the specific number of rows back.
   * Error text will include the SQL.
   * @param expected expected row count.
   * @param conf config for the select call.
   * @param header header option
   * @param sql template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the lines selected
   * @throws IOException failure
   */
  private List<String> expectSelected(
      final int expected,
      final Configuration conf,
      final String header,
      final String sql,
      final Object...args) throws Exception {
    conf.set(s3a(CSV_INPUT_HEADER), header);
    return verifySelectionCount(expected, sql(sql, args),
        selectCsvFile(conf, sql, args));
  }

  /**
   * Select from the CSV file.
   * @param conf config for the select call.
   * @param sql template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the lines selected
   * @throws IOException failure
   */
  private List<String> selectCsvFile(
      final Configuration conf,
      final String sql,
      final Object...args)
      throws Exception {

    return parseToLines(
        select(getFileSystem(), csvPath, conf, sql, args),
        1000);
  }


}
