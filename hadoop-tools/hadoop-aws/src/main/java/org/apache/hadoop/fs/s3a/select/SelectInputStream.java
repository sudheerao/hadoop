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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.internal.SdkFilterInputStream;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.io.IOUtils;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * An input stream for S3 Select return values.
 * This is simply an end-to-end GET request, without any
 * form of backwards seek or recovery from connectivity failures.
 *
 * Forward seek is remapped to skip().
 *
 * The normal S3 input counters are updated by this stream.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SelectInputStream extends FSInputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(SelectInputStream.class);

  /**
   * Same set of arguments as for an S3AInputStream.
   */
  private final S3ObjectAttributes objectAttributes;

  /**
   * Tracks the current position.
   */
  private long pos;

  /**
   * Closed bit. Volatile so reads are non-blocking.
   * Updates must be in a synchronized block to guarantee an atomic check and
   * set
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final InputStream wrappedStream;

  private final String bucket;

  private final String key;

  private final String uri;

  private final S3AReadOpContext readContext;

  private final S3AInstrumentation.InputStreamStatistics streamStatistics;

  /**
   * Create the stream.
   * The read attempt is initiated immediately.
   * @param readContext read context
   * @param s3Attributes object attributes from a HEAD request
   * @param selectResponse response from the already executed call
   * @throws IOException failure
   */
  public SelectInputStream(
      final S3AReadOpContext readContext,
      final S3ObjectAttributes s3Attributes,
      final SelectObjectContentResult selectResponse) throws IOException {
    Preconditions.checkArgument(isNotEmpty(s3Attributes.getBucket()),
        "No Bucket");
    Preconditions.checkArgument(isNotEmpty(s3Attributes.getKey()), "No Key");
    this.objectAttributes = s3Attributes;
    this.bucket = s3Attributes.getBucket();
    this.key = s3Attributes.getKey();
    this.uri = "s3a://" + this.bucket + "/" + this.key;
    this.readContext = readContext;
    this.streamStatistics = readContext.getInstrumentation()
        .newInputStreamStatistics();
    InputStream recordFrameInputStream = selectResponse.getPayload()
        .getRecordsInputStream();
    this.wrappedStream = new BufferedInputStream(recordFrameInputStream);
    // this stream is already opened, so mark as such in the statistics.
    streamStatistics.streamOpened();
  }


  @Override
  public void close() throws IOException {
    if (!closed.getAndSet(true)) {
      IOUtils.cleanupWithLogger(LOG, wrappedStream);
      streamStatistics.streamClose(false, 0);
      streamStatistics.close();
      super.close();
    }
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed.get()) {
      throw new IOException(uri + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  @Override
  public int available() throws IOException {
    checkNotClosed();
    return wrappedStream.available();
  }

  @Override
  public long skip(final long n) throws IOException {
    checkNotClosed();
    long skipped = wrappedStream.skip(n);
    pos += skipped;
    // treat as a forward skip for stats
    streamStatistics.seekForwards(skipped);
    return skipped;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized int read() throws IOException {
    checkNotClosed();
    int byteRead;
    try {
      byteRead = wrappedStream.read();
    } catch (EOFException e) {
      return -1;
    }

    if (byteRead >= 0) {
      incrementBytesRead(1);
    }
    return byteRead;
  }

  @SuppressWarnings("NullableProblems")
  @Override
  public int read(final byte[] buf, final int off, final int len)
      throws IOException {
    checkNotClosed();
    validatePositionedReadArgs(pos, buf, off, len);
    if (len == 0) {
      return 0;
    }

    int bytesRead;
    try {
      streamStatistics.readOperationStarted(pos, len);
      bytesRead = wrappedStream.read(buf, off, len);
    } catch (EOFException e) {
      streamStatistics.readException();
      // the base implementation swallows EOFs.
      return -1;
    }

    incrementBytesRead(bytesRead);
    streamStatistics.readOperationCompleted(len, bytesRead);
    return bytesRead;
  }

  // We don't support seek.
  @Override
  public void seek(long p) throws IOException {
    if (p == getPos()) {
      LOG.debug("ignoring seek to current position.");
    } else {
      throw unsupported("Seek");
    }
  }

  /**
   * Build an exception to raise when an operation is not supported here. 
   * @param action action which is unsupported.
   * @return an exception to throw.
   */
  protected PathIOException unsupported(final String action) {
    return new PathIOException(
        String.format("s3a://%s/%s", bucket, key),
        action + " not supported");
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw unsupported("Seek");
  }

  // Not supported.
  @Override
  public boolean markSupported() {
    return false;
  }

  @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
  @Override
  public void mark(int readLimit) {
    // Do nothing
  }

  @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
  @Override
  public void reset() throws IOException {
    throw unsupported("Mark");
  }

  /**
   * Can be used to provide abortion logic prior to throwing the
   * AbortedException. If the wrapped {@code InputStream} is also an instance
   * of this class, then it will also be aborted, otherwise this is a no-op.
   *
   * As the beta-release of the AWS SDK wraps the (abortable) input stream
   * in a java.io.SequenceInputStream, this doesn't work.
   */
  public void abort() {
    if (!closed.get() && wrappedStream instanceof SdkFilterInputStream) {
      LOG.debug("Aborting");
      ((SdkFilterInputStream) wrappedStream).abort();
    } else {
      LOG.debug("Ignoring abort()");
    }
  }

  @Override
  public int read(final long position,
      final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    if (position != getPos()) {
      throw unsupported("seek()");
    }
    return read(buffer, offset, length);
  }

  /**
   * Increment the bytes read counter if there is a stats instance
   * and the number of bytes read is more than zero.
   * This also updates the {@link #pos} marker by the same value.
   * @param bytesRead number of bytes read
   */
  private void incrementBytesRead(long bytesRead) {
    if (bytesRead > 0) {
      pos += bytesRead;
    }
    streamStatistics.bytesRead(bytesRead);
    if (readContext.getStats() != null && bytesRead > 0) {
      readContext.getStats().incrementBytesRead(bytesRead);
    }
  }

  public S3AInstrumentation.InputStreamStatistics getS3AStreamStatistics() {
    return streamStatistics;
  }

  /**
   * String value includes statistics as well as stream state.
   * <b>Important: there are no guarantees as to the stability
   * of this value.</b>
   * @return a string value for printing in logs/diagnostics
   */
  @Override
  @InterfaceStability.Unstable
  public String toString() {
    String s = streamStatistics.toString();
    synchronized (this) {
      final StringBuilder sb = new StringBuilder(
          "S3AInputStream{");
      sb.append(uri);
      sb.append("; state ").append(!closed.get() ? "open" : "closed");
      sb.append("; pos=").append(pos);
      sb.append('\n').append(s);
      sb.append('}');
      return sb.toString();
    }
  }
}
