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

import java.io.IOException;
import java.util.Locale;

import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;

/**
 * Class to do the select binding and build a select request from the supplied
 * arguments/configuration.
 *
 * This class is intended to be instantiated by the owning S3AFileSystem
 * instance to handle the construction of requests: IO is still done exclusively
 * in the filesystem.
 */
public class SelectBinding {

  /** Owning FS. */
  private final S3AFileSystem owner;

  /** Is S3 Select enabled? */
  private final boolean enabled;

  public SelectBinding(final S3AFileSystem owner) {
    this.owner = owner;
    this.enabled = getConf().getBoolean(FS_S3A_SELECT_SUPPORTED, true);
  }

  private Configuration getConf() {
    return owner.getConf();
  }

  /**
   * Is the service supported?
   * @return true iff select is enabled.
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Build a select request.
   * @param path the URI must contain the full query URI
   * @param expression the select expression.
   * @param conf config to extract other query options from
   * @return the request to serve
   * @throws IOException problem building the request
   */
  public SelectObjectContentRequest buildSelectRequest(Path path,
      String expression,
      final Configuration conf)
      throws IOException {
    Preconditions.checkState(isEnabled(),
        "S3 Select is not enabled for %s", path);
    return buildSelectOptionsFromExpression(expression, conf)
        .buildRequest(owner.newSelectRequest(path));
  }

  public SelectOptions buildSelectOptionsFromExpression(
      final String expression,
      final Configuration conf) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(expression),
        "No expression provided in parameter " + SELECT_EXPRESSION);

    SelectOptions selection = new SelectOptions();
    selection.setExpression(expandBackslashChars(expression));
    selection.setHeader(conf.get(HEADER, HEADER_OPT_DEFAULT)
        .toLowerCase(Locale.ENGLISH));
    selection.setFieldDelimiter(
        transformedString(conf, FIELD_DELIMITER, FIELD_DELIMITER_DEFAULT));
    selection.setRecordDelimiter(
        transformedString(conf, RECORD_DELIMITER, RECORD_DELIMITER_DEFAULT));
    selection.setCompression(conf.get(COMPRESSION, COMPRESSION_OPT_NONE));
    selection.setCommentMarker(
        transformedString(conf, COMMENT_MARKER, COMMENT_MARKER_DEFAULT));
    selection.setQuoteCharacter(
        transformedString(conf, QUOTE_CHARACTER, QUOTE_CHARACTER_DEFAULT));
    selection.setQuoteEscapeCharacter(transformedString(conf,
        QUOTE_ESCAPE_CHARACTER, QUOTE_ESCAPE_CHARACTER_DEFAULT));

    return selection;
  }

  static String transformedString(Configuration conf,
      String key, String defVal) {
    return expandBackslashChars(conf.getTrimmed(key, defVal));
  }

  static String expandBackslashChars(String r) {
    r = r.replace("\\n", "\n");
    r = r.replace("\\t", "\t");
    r = r.replace("\\r", "\r");
    return r;
  }
}
