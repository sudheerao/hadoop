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

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Writer for generating test CSV files.
 */
class CsvFile implements Closeable {


  /** constant to quote all columns. */
  public static final long ALL_QUOTES = 0x7fffffff;

  /** quote nothing. */
  public static final long NO_QUOTES = 0;

  private final Path path;

  private final PrintWriter out;

  private final String separator;

  private final String eol;

  private final String quote;

  CsvFile(final FileSystem fs,
      final Path path,
      boolean overwrite,
      final String separator,
      final String eol,
      final String quote) throws IOException {
    this.path = path;
    this.separator = Preconditions.checkNotNull(separator);
    this.eol = Preconditions.checkNotNull(eol);
    this.quote = Preconditions.checkNotNull(quote);
    out = new PrintWriter(fs.create(path, overwrite));
  }


  @Override
  public void close() throws IOException {
    out.close();
  }

  public Path getPath() {
    return path;
  }

  public String getSeparator() {
    return separator;
  }

  public String getEol() {
    return eol;
  }

  /**
   * Write a row.
   * Entries are quoted if the bit for that column is true.
   * @param quotes quote policy: every bit defines the rule for that element
   * @param columns columns to write
   * @return self for ease of chaining.
   * @throws IOException IO failure
   */
  public CsvFile row(long quotes, Object... columns) throws IOException {
    boolean isFirst = true;
    for (int i = 0; i < columns.length; i++) {
      if (i != 0) {
        out.write(separator);
      }
      boolean toQuote = (quotes & 1) == 1;
      // unsigned right shift to make next column flag @ position 0
      quotes = quotes >>> 1;
      if (toQuote) {
        out.write(quote);
      }
      out.write(columns[i].toString());
      if (toQuote) {
        out.write(quote);
      }
    }
    out.write(eol);
    return this;
  }

}
