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

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADVISE;
import static org.apache.hadoop.fs.s3a.S3AUtils.s3aset;

/**
 * Options related to S3 Select.
 *
 * These options are set for the entire filesystem unless overridden
 * as an option in the URI
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class SelectConstants {

  public static final String SELECT_UNSUPPORTED = "S3 Select is not supported";

  private SelectConstants() {
  }

  public static final String FS_S3A_SELECT = "fs.s3a.select.";


  /**
   * This is the big SQL expression: {@value}.
   * When used in an open() call, switch to a select operation.
   * This is only used in the open call, never in a filesystem configuration.
   */
  public static final String S3A_SELECT_SQL = "s3a:select.sql";

  /**
   * Does the FS Support S3 Select?
   * Value: {@value}.
   */
  public static final String S3_SELECT_CAPABILITY = S3A_SELECT_SQL;

  /**
   * Flag: is S3 select enabled?
   * Value: {@value}.
   */
  public static final String FS_S3A_SELECT_ENABLED = FS_S3A_SELECT
      + "enabled";

  /**
   * Prefix for all CSV input options.
   */
  public static final String FS_S3A_SELECT_INPUT_CSV =
      "fs.s3a.select.input.csv.";

  /**
   * String which indicates the row is actually a comment.
   */
  public static final String CSV_INPUT_COMMENT_MARKER =
      FS_S3A_SELECT_INPUT_CSV + "comment.marker";

  /**
   * Default marker.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_COMMENT_MARKER_DEFAULT = "#";

  /**
   * Record delimiter. CR, LF, etc.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_RECORD_DELIMITER =
      FS_S3A_SELECT_INPUT_CSV + "record.delimiter";

  /**
   * Default delimiter
   * Value: {@value}.
   */
  public static final String CSV_INPUT_RECORD_DELIMITER_DEFAULT = "\n";

  /**
   * Field delimiter.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_INPUT_FIELD_DELIMITER =
      FS_S3A_SELECT_INPUT_CSV + "field.delimiter";

  /**
   * Default field delimiter.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_FIELD_DELIMITER_DEFAULT = ",";

  /**
   * Quote Character.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_QUOTE_CHARACTER =
      FS_S3A_SELECT_INPUT_CSV + "quote.character";

  /**
   * Default Quote Character.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_QUOTE_CHARACTER_DEFAULT = "\"";

  /**
   * Character to escape quotes.
   * If empty: no escaping.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_QUOTE_ESCAPE_CHARACTER =
      FS_S3A_SELECT_INPUT_CSV + "quote.escape.character";

  /**
   * Default quote escape character.
   * Value: {@value}.
   */
  public static final String CSV_INPUT_QUOTE_ESCAPE_CHARACTER_DEFAULT = "";

  /**
   * How should headers be used?
   * Value: {@value}.
   */
  public static final String CSV_INPUT_HEADER =
      FS_S3A_SELECT_INPUT_CSV + "header";


  /**
   * No header: first row is data.
   * Value: {@value}.
   */
  public static final String HEADER_OPT_NONE = "none";

  /**
   * Ignore the header.
   * Value: {@value}.
   */
  public static final String HEADER_OPT_IGNORE = "ignore";

  /**
   * Use the header.
   * Value: {@value}.
   */
  public static final String HEADER_OPT_USE = "use";

  /**
   * Default header mode: {@value}.
   */
  public static final String CSV_INPUT_HEADER_OPT_DEFAULT = HEADER_OPT_IGNORE;

  /**
   * How is the input compressed? This applies to all formats.
   * Value: {@value}.
   */
  public static final String INPUT_COMPRESSION = FS_S3A_SELECT
      + "input.compression";

  /**
   * No compression.
   * Value: {@value}.
   */
  public static final String COMPRESSION_OPT_NONE = "none";

  /**
   * Gzipped.
   * Value: {@value}.
   */
  public static final String COMPRESSION_OPT_GZIP = "gzip";

  /**
   * An unmodifiable set listing the options
   * supported in {@code openFile()}.
   */
  public static final Set<String> SELECT_OPTIONS =
      // when adding to this, please keep in alphabetical order after the
      // common options and the SQL.
      Collections.unmodifiableSet(
          s3aset(Arrays.asList(
              INPUT_FADVISE,
              S3A_SELECT_SQL,
              CSV_INPUT_COMMENT_MARKER,
              CSV_INPUT_INPUT_FIELD_DELIMITER,
              CSV_INPUT_HEADER,
              CSV_INPUT_QUOTE_CHARACTER,
              CSV_INPUT_QUOTE_ESCAPE_CHARACTER,
              CSV_INPUT_RECORD_DELIMITER,
              INPUT_COMPRESSION
          )));
}
