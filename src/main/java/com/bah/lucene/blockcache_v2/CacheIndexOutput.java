/**
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
package com.bah.lucene.blockcache_v2;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;

import com.bah.lucene.buffer.BufferStore;

public class CacheIndexOutput extends IndexOutput {

  private final IndexOutput _indexOutput;
  private final Cache _cache;
  private final String _fileName;
  private final CacheDirectory _directory;
  private final long _fileId;
  private final int _fileBufferSize;
  private final int _cacheBlockSize;
  private final Checksum _digest;

  private long _position;
  private byte[] _buffer;
  private int _bufferPosition;

  public CacheIndexOutput(CacheDirectory directory, String fileName, IndexOutput indexOutput, Cache cache)
      throws IOException {
    _cache = cache;
    _directory = directory;
    _fileName = fileName;
    _fileBufferSize = _cache.getFileBufferSize(_directory, _fileName);
    _cacheBlockSize = _cache.getCacheBlockSize(_directory, _fileName);
    _fileId = _cache.getFileId(_directory, _fileName);
    _indexOutput = indexOutput;
    _buffer = BufferStore.takeBuffer(_cacheBlockSize);
    _digest = new BufferedChecksum(new CRC32());
  }

  @Override
  public void writeByte(byte b) throws IOException {
    tryToFlush();
    _buffer[_bufferPosition] = b;
    _bufferPosition++;
    _position++;
  }

  @Override
  public void writeBytes(byte[] b, int offset, int len) throws IOException {
    while (len > 0) {
      tryToFlush();
      int remaining = remaining();
      int length = Math.min(len, remaining);
      System.arraycopy(b, offset, _buffer, _bufferPosition, length);
      _bufferPosition += length;
      _position += length;
      len -= length;
      offset += length;
    }
  }

  private int remaining() {
    return _cacheBlockSize - _bufferPosition;
  }

  private void tryToFlush() throws IOException {
    if (remaining() == 0) {
      flushInternal();
    }
  }

  private void flushInternal() throws IOException {
    int length = _cacheBlockSize - remaining();
    if (length == 0) {
      return;
    }
    CacheValue cacheValue = _cache.newInstance(_directory, _fileName);
    writeBufferToOutputStream(length);
    cacheValue.write(0, _buffer, 0, length);
    long blockId = (_position - length) / _cacheBlockSize;
    cacheValue = cacheValue.trim(length);
    _cache.put(new CacheKey(_fileId, blockId), cacheValue);
    _bufferPosition = 0;
  }

  private void writeBufferToOutputStream(int len) throws IOException {
    int offset = 0;
    while (len > 0) {
      int length = Math.min(_fileBufferSize, len);
      _indexOutput.writeBytes(_buffer, offset, length);
      _digest.update(_buffer, offset, length);
      len -= length;
      offset += length;
    }
  }

  @Override
  public void close() throws IOException {
    flushInternal();
    _indexOutput.flush();
    _indexOutput.close();
    BufferStore.putBuffer(_buffer);
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public long getFilePointer() {
    return _position;
  }

  @Override
  public long length() throws IOException {
    return getFilePointer();
  }

  @Override
  public long getChecksum() throws IOException {
    return _digest.getValue();
  }
}
