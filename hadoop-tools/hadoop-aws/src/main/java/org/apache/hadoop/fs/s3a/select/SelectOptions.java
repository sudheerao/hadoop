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

import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.FileHeaderInfo;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.QuoteFields;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;

import org.apache.commons.lang3.StringUtils;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.commit.ValidationFailure.verify;

/**
 * Options for a S3 Select call.
 */
public class SelectOptions {

  private String compression = "";

  private String expression = "";

  private String recordDelimiter = "\n";

  private String fieldDelimiter = ",";

  private String quoteCharacter = "\"";

  private String quoteEscapeCharacter = "'";

  private String commentMarker = "#";

  private String header;

  public String getHeader() {
    return header;
  }

  public void setHeader(final String h) {
    this.header = h;
  }

  public String getCompression() {
    return compression;
  }

  public SelectOptions setCompression(final String co) {
    this.compression = co;
    return this;
  }

  public String getExpression() {
    return expression;
  }

  public SelectOptions setExpression(final String ex) {
    this.expression = ex;
    return this;
  }

  public String getFieldDelimiter() {
    return fieldDelimiter;
  }

  public SelectOptions setFieldDelimiter(final String delimiter) {
    this.fieldDelimiter = delimiter;
    return this;
  }

  public String getQuoteEscapeCharacter() {
    return quoteEscapeCharacter;
  }

  public SelectOptions setQuoteEscapeCharacter(final String ch) {
    this.quoteEscapeCharacter = ch;
    return this;
  }

  public String getCommentMarker() {
    return commentMarker;
  }

  public SelectOptions setCommentMarker(final String m) {
    this.commentMarker = m;
    return this;
  }

  public String getRecordDelimiter() {
    return recordDelimiter;
  }

  public SelectOptions setRecordDelimiter(final String delimiter) {
    this.recordDelimiter = delimiter;
    return this;
  }

  public String getQuoteCharacter() {
    return quoteCharacter;
  }

  public SelectOptions setQuoteCharacter(final String ch) {
    this.quoteCharacter = ch;
    return this;
  }

  public void validate() throws IOException {
    verify(isNotEmpty(expression), "No expression");
    verify(isEmpty(header) || null != getHeaderInfo(),
        "Bad header option \"%s\"", header);
    verify(isEmpty(compression) || null != getInputCompression(),
        "Bad compression \"%s\"", compression);

  }

  private FileHeaderInfo getHeaderInfo() {
    return FileHeaderInfo.fromValue(header.toUpperCase(Locale.ENGLISH));
  }

  private CompressionType getInputCompression() {
    return CompressionType.fromValue(compression.toUpperCase(Locale.ENGLISH));
  }

  /**
   * Validate the params then build up the S3 request.
   * @param request request preconfigured with bucket and key
   * @return the constructed request
   * @throws IOException validation failure
   */
  public SelectObjectContentRequest buildRequest(
      SelectObjectContentRequest request) throws IOException {
    validate();

    request.setExpressionType(ExpressionType.SQL);
    request.setExpression(expression);

    // CSV input
    CSVInput csv = new CSVInput();
    csv.setFieldDelimiter(fieldDelimiter);
    csv.setRecordDelimiter(recordDelimiter);
    csv.setComments(commentMarker);
    if (StringUtils.isNotEmpty(quoteEscapeCharacter)) {
      csv.setQuoteEscapeCharacter(quoteEscapeCharacter);
    }
    csv.setFileHeaderInfo(getHeaderInfo());
    InputSerialization inputSerialization
        = new InputSerialization();
    inputSerialization.setCsv(csv);
    if (isNotEmpty(compression)) {
      inputSerialization.setCompressionType(getInputCompression());
    }
    // output is CSV, always
    OutputSerialization outputSerialization
        = new OutputSerialization();
    CSVOutput csvOut = new CSVOutput();
    csvOut.setQuoteCharacter("\"");
    csvOut.setQuoteFields(QuoteFields.ALWAYS);

    // maybe support output ser parameters here.
    outputSerialization.setCsv(csvOut);
    request.setInputSerialization(inputSerialization);
    request.setOutputSerialization(outputSerialization);
    return request;
  }

}
