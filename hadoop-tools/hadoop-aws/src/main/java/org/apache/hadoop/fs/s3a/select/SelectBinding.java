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

import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.S3AUtils.s3a;
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

  private static final Logger LOG =
      LoggerFactory.getLogger(SelectBinding.class);
  /** Owning FS. */
  private final S3AFileSystem owner;

  /** Is S3 Select enabled? */
  private final boolean enabled;

  /**
   * Constructor.
   * @param owner owning FS.
   */
  public SelectBinding(final S3AFileSystem owner) {
    this.owner = checkNotNull(owner);
    this.enabled = getConf().getBoolean(FS_S3A_SELECT_ENABLED, true);
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
  public SelectObjectContentRequest buildSelectRequest(
      final Path path,
      final String expression,
      final Configuration conf)
      throws IOException {
    Preconditions.checkState(isEnabled(),
        "S3 Select is not enabled for %s", path);
    return buildSelectOptionsFromConfigurations(expression, conf)
        .buildRequest(owner.newSelectRequest(path));
  }

  /**
   * Build the select request from the configuration built up
   * in {@link S3AFileSystem#openFile(Path)} and the default
   * options in the cluster configuration.
   *
   * Options are picked up in the following order.
   * <ol>
   *   <li>The s3a: - prefixed options in {@code openFileOptions}</li>
   *   <li>the non-prefixed options in the owning filesystem
   *   configuration.</li>
   *   <li>The default values in {@link SelectConstants}</li>
   * </ol>
   * @param expression SQL expression
   * @param builderOpts the options which came in from the openFile builder.
   * @return the options for the select request.
   */
  private SelectOptions buildSelectOptionsFromConfigurations(
      final String expression,
      final Configuration builderOpts) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(expression),
        "No expression provided in parameter " + S3A_SELECT_SQL);

    Configuration fsConf = owner.getConf();

    SelectOptions selection = new SelectOptions();

    selection.setHeader(
        opt(builderOpts, fsConf, CSV_INPUT_HEADER,
            CSV_INPUT_HEADER_OPT_DEFAULT, true)
        .toLowerCase(Locale.ENGLISH));
    selection.setFieldDelimiter(
        xopt(builderOpts, fsConf, CSV_INPUT_INPUT_FIELD_DELIMITER,
            CSV_INPUT_FIELD_DELIMITER_DEFAULT));
    selection.setRecordDelimiter(
        xopt(builderOpts, fsConf, CSV_INPUT_RECORD_DELIMITER,
            CSV_INPUT_RECORD_DELIMITER_DEFAULT));
    selection.setCompression(
        opt(builderOpts, fsConf, INPUT_COMPRESSION, COMPRESSION_OPT_NONE, true)
            .toUpperCase(Locale.ENGLISH));
    selection.setCommentMarker(
        xopt(builderOpts, fsConf, CSV_INPUT_COMMENT_MARKER,
            CSV_INPUT_COMMENT_MARKER_DEFAULT));
    selection.setQuoteCharacter(
        xopt(builderOpts, fsConf, CSV_INPUT_QUOTE_CHARACTER,
            CSV_INPUT_QUOTE_CHARACTER_DEFAULT));
    selection.setQuoteEscapeCharacter(xopt(builderOpts, fsConf,
        CSV_INPUT_QUOTE_ESCAPE_CHARACTER,
        CSV_INPUT_QUOTE_ESCAPE_CHARACTER_DEFAULT));
    selection.setExpression(expandBackslashChars(expression));

    return selection;
  }

  /**
   * Stringify the given SelectObjectContentRequest, as its
   * toString() operator doesn't.
   * @param request request to convert to a string
   * @return a string to print. Does not contain secrets.
   */
  public static String toString(final SelectObjectContentRequest request) {
    StringBuilder sb = new StringBuilder();
    sb.append("SelectObjectContentRequest{")
        .append("bucket name=").append(request.getBucketName())
        .append("; key=").append(request.getKey())
        .append("; expressionType=").append(request.getExpressionType())
        .append("; expression=").append(request.getExpression());
    InputSerialization input = request.getInputSerialization();
    if (input != null) {
      sb.append("; Input")
          .append(input.toString());
    } else {
      sb.append("; Input Serialization: none");
    }
    OutputSerialization out = request.getOutputSerialization();
    if (out != null) {
      sb.append("; Output")
          .append(out.toString());
    } else {
      sb.append("; Output Serialization: none");
    }
    return sb.append("}").toString();
  }

  /**
   * Resolve an option.
   * @param builderOpts the options which came in from the openFile builder.
   * @param fsConf configuration of the owning FS.
   * @param base base option (no s3a: prefix)
   * @param defVal default value. Must not be null.
   * @param trim should the result be trimmed.
   * @return the possibly trimmed value.
   */
  private static String opt(Configuration builderOpts,
      Configuration fsConf,
      String base,
      String defVal,
      boolean trim) {
    String fsVal = fsConf.get(base, defVal);
    String r = builderOpts.get(s3a(base), fsVal);
    return trim ? r.trim() : r;
  }

  /**
   * Get a transformed opt.
   * These are not trimmed, so whitespace is significant.
   * @param selectOpts options in the select call
   * @param fsConf filesystem conf
   * @param base base option name
   * @param defVal default value
   * @return the transformed value
   */
  private static String xopt(Configuration selectOpts,
      Configuration fsConf,
      String base,
      String defVal) {
    return expandBackslashChars(
        opt(selectOpts, fsConf, base, defVal, false));
  }

  /**
   * Perform escaping.
   * @param src source string.
   * @return the replaced value
   */
  static String expandBackslashChars(String src) {
    return src.replace("\\n", "\n")
        .replace("\\\"", "\"")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
        .replace("\\\"", "\"")
          // backslash substitution must come last
        .replace("\\\\", "\\");
  }

}
