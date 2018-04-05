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

/**
 * Options related to S3 Select.
 *
 * These options are set for the entire filesystem unless overridden
 * as an option in the URI
 */
public final class SelectConstants {

  private SelectConstants() {
  }

  public static final String FS_S3A_SELECT = "fs.s3a.select.";
  public static final String FS_S3A_SELECT_SUPPORTED = FS_S3A_SELECT
      + "supported";

  public static final String COMMENT_MARKER = FS_S3A_SELECT + "comment.marker";
  public static final String COMMENT_MARKER_DEFAULT = "#";

  public static final String RECORD_DELIMITER = FS_S3A_SELECT
      + "record.delimiter";

  public static final String RECORD_DELIMITER_DEFAULT = "\n";

  public static final String FIELD_DELIMITER = FS_S3A_SELECT
      + "field.delimiter";

  public static final String FIELD_DELIMITER_DEFAULT = ",";

  public static final String QUOTE_CHARACTER = FS_S3A_SELECT
      + "quote.character";

  public static final String QUOTE_CHARACTER_DEFAULT = "\"";

  public static final String QUOTE_ESCAPE_CHARACTER = FS_S3A_SELECT
      + "quote.escape.character";

  public static final String QUOTE_ESCAPE_CHARACTER_DEFAULT = "";

  public static final String COMPRESSION = FS_S3A_SELECT + "compression";

  public static final String COMPRESSION_OPT_NONE = "NONE";
  public static final String COMPRESSION_OPT_GZIP = "GZIP";

  public static final String HEADER = FS_S3A_SELECT + "header";
  public static final String HEADER_OPT_NONE = "none";
  public static final String HEADER_OPT_IGNORE = "ignore";
  public static final String HEADER_OPT_USE = "use";

  /**
   * Default header mode: {@value}.
   */
  public static final String HEADER_OPT_DEFAULT = HEADER_OPT_IGNORE;

  public static final String SELECT_EXPRESSION = FS_S3A_SELECT + "expression";

  /**
   * Does the FS Support S3 Select?
   */
  public static final String S3_SELECT_CAPABILITY = "s3a:s3-select";
}
