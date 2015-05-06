/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bah.lucene.buffer;

import java.io.EOFException;
import java.io.IOException;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;

/**
 * Implementation of an IndexInput that reads from a portion of a file.
 */
public class SlicedIndexInput extends BufferedIndexInput {
  IndexInput base;
  long fileOffset;
  long length;

  public SlicedIndexInput(final String sliceDescription, final IndexInput base,
      final long fileOffset, final long length) {
    this(sliceDescription, base, fileOffset, length,
        BufferedIndexInput.BUFFER_SIZE);
  }

  public SlicedIndexInput(final String sliceDescription, final IndexInput base,
      final long fileOffset, final long length, int readBufferSize) {
    super("SlicedIndexInput(" + sliceDescription + " in " + base + " slice="
        + fileOffset + ":" + (fileOffset + length) + ")", readBufferSize);
    this.base = base.clone();
    this.fileOffset = fileOffset;
    this.length = length;
  }

  @Override
  public SlicedIndexInput clone() {
    SlicedIndexInput clone = (SlicedIndexInput) super.clone();
    clone.base = base.clone();
    clone.fileOffset = fileOffset;
    clone.length = length;
    return clone;
  }

  /**
   * Expert: implements buffer refill. Reads bytes from the current position in
   * the input.
   * 
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len the number of bytes to read
   */
  @Override
  protected void readInternal(byte[] b, int offset, int len) throws IOException {
    long start = getFilePointer();
    if (start + len > length)
      throw new EOFException("read past EOF: " + this);
    base.seek(fileOffset + start);
    base.readBytes(b, offset, len, false);
  }

  /**
   * Expert: implements seek. Sets current position in this file, where the next
   * {@link #readInternal(byte[],int,int)} will occur.
   * 
   * @see #readInternal(byte[],int,int)
   */
  @Override
  protected void seekInternal(long pos) {
  }

  /** Closes the stream to further operations. */
  @Override
  public void close() throws IOException {
    base.close();
  }

  @Override
  public long length() {
    return length;
  }
}
