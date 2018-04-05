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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.commit.Duration;
import org.apache.hadoop.fs.s3a.commit.DurationInfo;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.util.ExitUtil;

import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.*;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;

/**
 * This is a CLI tool for the select operation, which is available
 * through the S3Guard command.
 *
 * Usage:
 * <pre>
 *   hadoop s3guard select [options] Path Statement
 * </pre>
 */
public class SelectTool extends S3GuardTool {

  private static final Logger LOG =
      LoggerFactory.getLogger(SelectInputStream.class);

  public static final String NAME = "select";

  public static final String PURPOSE = "make an S3 Select call";

  private static final String USAGE = NAME
      + " [OPTIONS] [-limit lines] [-header (use|none|ignore)]"
      + " [-out file]"
      + " [-compression (gzip|none)]"
      + "  <PATH> <SELECT QUERY>\n"
      + "\t" + PURPOSE + "\n\n";

  public static final String OPT_LIMIT = "limit";

  public static final String OPT_HEADER = "header";

  public static final String OPT_OUTPUT = "out";

  public static final String OPT_COMPRESSION = "compression";

  public SelectTool(Configuration conf) {
    super(conf);
    // read capacity.
    getCommandFormat().addOptionWithValue(OPT_LIMIT);
    getCommandFormat().addOptionWithValue(OPT_OUTPUT);
    getCommandFormat().addOptionWithValue(OPT_HEADER);
    getCommandFormat().addOptionWithValue(OPT_COMPRESSION);
    getCommandFormat().addOptionWithValue(OPT_OUTPUT);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getUsage() {
    return USAGE;
  }

  public int run(String[] args, PrintStream out)
      throws InterruptedException, IOException {
    List<String> parsedArgs = parseArgs(args);
    if (parsedArgs.size() != 2) {
      errorln(getUsage());
      throw invalidArgs("Wrong number of arguments");
    }

    // read mandatory arguments
    String file = parsedArgs.get(0);
    Path path = new Path(file);
    String expression = parsedArgs.get(1);

    println(out, "selecting file %s with query %s",
        path, expression);

    // and the optional arguments to adjust the configuration.
    Configuration initialConf = getConf();
    CommandFormat command = getCommandFormat();
    String header = command.getOptValue(OPT_HEADER);
    if (isNotEmpty(header)) {
      println(out, "Using header option %s", header);
      initialConf.set(HEADER, header, "cli");
    }
    String output = command.getOptValue(OPT_OUTPUT);
    File outfile = null;
    if (isNotEmpty(output)) {
      outfile = new File(output).getAbsoluteFile();
      println(out, "Saving output to %s", outfile);
    }
    String lineLimitArg = command.getOptValue(OPT_LIMIT);
    if (isNotEmpty(lineLimitArg)) {
      int lineLimit = Integer.parseInt(lineLimitArg);
      println(out, "Using line limit %s", lineLimit);
      if (expression.toLowerCase(Locale.ENGLISH).contains(" limit ")) {
        println(out, "line limit already specified in SELECT expression");
      } else {
        expression = expression + " LIMIT " + lineLimitArg;
      }
    }
    String compression = command.getOptValue(OPT_COMPRESSION);
    if (isNotEmpty(compression)) {
      initialConf.set(COMPRESSION,
          compression.toUpperCase(Locale.ENGLISH), "cli");
    }

    // now bind to the filesystem
    S3AFileSystem fs = (S3AFileSystem) FileSystem.newInstance(
        toUri(file), initialConf);
    setFilesystem(fs);
    if (!fs.hasCapability(S3_SELECT_CAPABILITY)) {
      // capability disabled
      throw new ExitUtil.ExitException(EXIT_SERVICE_UNAVAILABLE,
          "S3 Select is not supported on " + file);
    }
    Configuration conf = fs.getConf();
    long lines = 0;

    Duration duration = new Duration();
    conf.set(SELECT_EXPRESSION, expression, "Command line");

    long bytesReadOrig = getStatistic(Statistic.STREAM_SEEK_BYTES_READ);

    // open and scan the stream.
    try (FSDataInputStream stream = fs.open(path)) {

      if (outfile == null) {
        // logging to console

        Scanner scanner =
            new Scanner(
                new BufferedReader(
                    new InputStreamReader(stream, "UTF-8")));
        scanner.useDelimiter("\n");
        while (scanner.hasNextLine()) {
          lines++;
          String l = scanner.nextLine();
          println(out, "%s", l);
        }
      } else {
        // straight dump of whole file, no interpretation of line ends or
        // anything else
        FileUtils.copyInputStreamToFile(stream, outfile);
      }

      // close the stream.
      // this will take time if there's a lot of data remaining
      try (DurationInfo d = new DurationInfo(LOG, "Closing stream")) {
        stream.close();
      }

      // get the #of bytes actually read
      long bytesRead = getStatistic(Statistic.STREAM_SEEK_BYTES_READ)
          - bytesReadOrig;

      // generate a meaningful result depending on the operation
      String result = outfile == null
          ? String.format("%s lines", lines)
          : String.format("%s bytes", bytesRead);

      // print some statistics
      duration.finished();
      println(out, "Read %s in time %s",
          result, duration.getDurationString());
      // rely on the fact that the S3A streams print their statistics

      println(out, "Bytes Read: %,d", bytesRead);

      println(out, "Bandwidth: %,.1f MB/s",
          bandwidthMBs(bytesRead, duration.value()));

      LOG.debug("Statistics {}", stream);
    } finally {
      conf.unset(SELECT_EXPRESSION);
    }

    out.flush();

    return EXIT_SUCCESS;
  }

  private long getStatistic(final Statistic stat) {
    return getFilesystem().getInstrumentation().getCounterValue(
        stat);
  }

  /**
   * Work out the bandwidth in MB/s.
   * @param bytes bytes
   * @param durationNS duration in nanos
   * @return the number of megabytes/second of the recorded operation
   */
  public static double bandwidthMBs(long bytes, long durationNS) {
    return durationNS > 0 ?
        (bytes / (1024.0 * 1024) * 1.0e9 / durationNS)
        : 0;
  }
}
