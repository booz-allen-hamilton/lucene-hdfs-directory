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
package org.apache.blur.store.blockcache_v2;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;
import org.apache.blur.store.blockcache_v2.cachevalue.ByteArrayCacheValue;
import org.apache.blur.store.blockcache_v2.cachevalue.UnsafeCacheValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.store.IOContext;

import java.io.Closeable;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BaseCache extends Cache implements Closeable {

  private static final Log LOG = LogFactory.getLog(BaseCache.class);
  private static final long _1_MINUTE = TimeUnit.MINUTES.toMillis(1);
  protected static final long _10_SECOND = TimeUnit.SECONDS.toMillis(10);

  public enum STORE {
    ON_HEAP, OFF_HEAP
  }

  class BaseCacheEvictionListener implements EvictionListener<CacheKey, CacheValue> {
    @Override
    public void onEviction(CacheKey key, CacheValue value) {
      addToReleaseQueue(key, value);
    }
  }

  class BaseCacheWeigher implements Weigher<CacheValue> {
    @Override
    public int weightOf(CacheValue value) {
      return value.size();
    }
  }

  static class ReleaseEntry {
    CacheKey _key;
    CacheValue _value;
    final long _createTime = System.currentTimeMillis();

    @Override
    public String toString() {
      return "ReleaseEntry [_key=" + _key + ", _value=" + _value + ", _createTime=" + _createTime + "]";
    }

    public boolean hasLivedToLong(long warningTimeForEntryCleanup) {
      long now = System.currentTimeMillis();
      if (_createTime + warningTimeForEntryCleanup < now) {
        return true;
      }
      return false;
    }

  }

  private final ConcurrentLinkedHashMap<CacheKey, CacheValue> _cacheMap;
  private final FileNameFilter _readFilter;
  private final FileNameFilter _writeFilter;
  private final STORE _store;
  private final Size _cacheBlockSize;
  private final Size _fileBufferSize;
  private final Map<FileIdKey, Long> _fileNameToId = new ConcurrentHashMap<FileIdKey, Long>();
  private final Map<Long, FileIdKey> _oldFileNameIdMap = new ConcurrentHashMap<Long, FileIdKey>();
  private final AtomicLong _fileId = new AtomicLong();
  private final Quiet _quiet;
  private final Thread _oldFileDaemonThread;
  private final Thread _oldCacheValueDaemonThread;
  private final AtomicBoolean _running = new AtomicBoolean(true);
  private final BlockingQueue<ReleaseEntry> _releaseQueue;
  private final long _warningTimeForEntryCleanup = TimeUnit.MINUTES.toMillis(60);

  public BaseCache(long totalNumberOfBytes, Size fileBufferSize, Size cacheBlockSize, FileNameFilter readFilter,
      FileNameFilter writeFilter, Quiet quiet, STORE store) {
    _cacheMap = new ConcurrentLinkedHashMap.Builder<CacheKey, CacheValue>().weigher(new BaseCacheWeigher())
        .maximumWeightedCapacity(totalNumberOfBytes).listener(new BaseCacheEvictionListener()).build();
    _fileBufferSize = fileBufferSize;
    _readFilter = readFilter;
    _writeFilter = writeFilter;
    _store = store;
    _cacheBlockSize = cacheBlockSize;
    _quiet = quiet;
    _releaseQueue = new LinkedBlockingQueue<ReleaseEntry>();
    _oldFileDaemonThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          cleanupOldFiles();
          try {
            Thread.sleep(_1_MINUTE);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    _oldFileDaemonThread.setDaemon(true);
    _oldFileDaemonThread.setName("BaseCacheOldFileCleanup");
    _oldFileDaemonThread.setPriority(Thread.MIN_PRIORITY);
    _oldFileDaemonThread.start();

    _oldCacheValueDaemonThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (_running.get()) {
          cleanupOldCacheValues();
          try {
            Thread.sleep(_10_SECOND);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    });
    _oldCacheValueDaemonThread.setDaemon(true);
    _oldCacheValueDaemonThread.setName("BaseCacheCleanupCacheValues");
    _oldCacheValueDaemonThread.start();
  }

  protected void cleanupOldCacheValues() {
    Iterator<ReleaseEntry> iterator = _releaseQueue.iterator();
    Map<Long, FileIdKey> entriesToCleanup = new HashMap<Long, FileIdKey>(_oldFileNameIdMap);
    while (iterator.hasNext()) {
      ReleaseEntry entry = iterator.next();
      CacheKey key = entry._key;
      long fileId = key.getFileId();
      // Still referenced
      entriesToCleanup.remove(fileId);

      CacheValue value = entry._value;
      if (value.refCount() == 0) {
        value.release();
        iterator.remove();
        long capacity = _cacheMap.capacity();
        _cacheMap.setCapacity(capacity + value.size());
      } else if (entry.hasLivedToLong(_warningTimeForEntryCleanup)) {
        FileIdKey fileIdKey = _oldFileNameIdMap.get(fileId);
        LOG.warn(MessageFormat.format("CacheValue has not been released [{0}] for [{1}] for over [{2} ms]", entry, fileIdKey,
            _warningTimeForEntryCleanup));
      }
    }
    for (Long l : entriesToCleanup.keySet()) {
      _oldFileNameIdMap.remove(l);
    }
  }

  protected void cleanupOldFiles() {
    LOG.debug("Cleanup old files from cache.");
    Set<Long> validFileIds = new HashSet<Long>(_fileNameToId.values());
    for (CacheKey key : _cacheMap.keySet()) {
      long fileId = key.getFileId();
      if (validFileIds.contains(fileId)) {
        CacheValue remove = _cacheMap.remove(key);
        if (remove != null) {
          addToReleaseQueue(key, remove);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    _running.set(false);
    _cacheMap.clear();
    _oldFileDaemonThread.interrupt();
    _oldCacheValueDaemonThread.interrupt();
    for (ReleaseEntry entry : _releaseQueue) {
      entry._value.release();
    }
  }

  private void addToReleaseQueue(CacheKey key, CacheValue value) {
    if (value != null) {
      if (value.refCount() == 0) {
        value.release();
        return;
      }
      long capacity = _cacheMap.capacity();
      _cacheMap.setCapacity(capacity - value.size());

      ReleaseEntry releaseEntry = new ReleaseEntry();
      releaseEntry._key = key;
      releaseEntry._value = value;

      LOG.debug(MessageFormat.format("CacheValue was not released [{0}]", releaseEntry));
      _releaseQueue.add(releaseEntry);
    }
  }

  @Override
  public boolean shouldBeQuiet(CacheDirectory directory, String fileName) {
    return _quiet.shouldBeQuiet(directory, fileName);
  }

  @Override
  public CacheValue newInstance(CacheDirectory directory, String fileName, int cacheBlockSize) {
    switch (_store) {
    case ON_HEAP:
      return new ByteArrayCacheValue(cacheBlockSize);
    case OFF_HEAP:
      return new UnsafeCacheValue(cacheBlockSize);
    default:
      throw new RuntimeException("Unknown type [" + _store + "]");
    }
  }

  @Override
  public long getFileId(CacheDirectory directory, String fileName) throws IOException {
    FileIdKey cachedFileName = getCacheFileName(directory, fileName);
    Long id = _fileNameToId.get(cachedFileName);
    if (id != null) {
      return id;
    }
    long newId = _fileId.incrementAndGet();
    _fileNameToId.put(cachedFileName, newId);
    _oldFileNameIdMap.put(newId, cachedFileName);
    return newId;
  }

  @Override
  public void removeFile(CacheDirectory directory, String fileName) throws IOException {
    FileIdKey cachedFileName = getCacheFileName(directory, fileName);
    _fileNameToId.remove(cachedFileName);
  }

  private FileIdKey getCacheFileName(CacheDirectory directory, String fileName) throws IOException {
    long fileModified = directory.getFileModified(fileName);
    return new FileIdKey(directory.getDirectoryName(), fileName, fileModified);
  }

  @Override
  public int getCacheBlockSize(CacheDirectory directory, String fileName) {
    return _cacheBlockSize.getSize(directory, fileName);
  }

  @Override
  public int getFileBufferSize(CacheDirectory directory, String fileName) {
    return _fileBufferSize.getSize(directory, fileName);
  }

  @Override
  public boolean cacheFileForReading(CacheDirectory directory, String fileName, IOContext context) {
    return _readFilter.accept(directory, fileName);
  }

  @Override
  public boolean cacheFileForWriting(CacheDirectory directory, String fileName, IOContext context) {
    return _writeFilter.accept(directory, fileName);
  }

  @Override
  public CacheValue get(CacheKey key) {
    return _cacheMap.get(key);
  }

  @Override
  public CacheValue getQuietly(CacheKey key) {
    return _cacheMap.getQuietly(key);
  }

  @Override
  public void put(CacheKey key, CacheValue value) {
    _cacheMap.put(key, value);
  }

  @Override
  public void releaseDirectory(String directoryName) {
    Set<Entry<FileIdKey, Long>> entrySet = _fileNameToId.entrySet();
    Iterator<Entry<FileIdKey, Long>> iterator = entrySet.iterator();
    while (iterator.hasNext()) {
      Entry<FileIdKey, Long> entry = iterator.next();
      FileIdKey fileIdKey = entry.getKey();
      if (fileIdKey._directoryName.equals(directoryName)) {
        iterator.remove();
      }
    }
  }

  static class FileIdKey {
    final String _directoryName;
    final String _fileName;
    final long _lastModified;

    FileIdKey(String directoryName, String fileName, long lastModified) {
      _directoryName = directoryName;
      _fileName = fileName;
      _lastModified = lastModified;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((_directoryName == null) ? 0 : _directoryName.hashCode());
      result = prime * result + ((_fileName == null) ? 0 : _fileName.hashCode());
      result = prime * result + (int) (_lastModified ^ (_lastModified >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      FileIdKey other = (FileIdKey) obj;
      if (_directoryName == null) {
        if (other._directoryName != null)
          return false;
      } else if (!_directoryName.equals(other._directoryName))
        return false;
      if (_fileName == null) {
        if (other._fileName != null)
          return false;
      } else if (!_fileName.equals(other._fileName))
        return false;
      if (_lastModified != other._lastModified)
        return false;
      return true;
    }
  }

}
