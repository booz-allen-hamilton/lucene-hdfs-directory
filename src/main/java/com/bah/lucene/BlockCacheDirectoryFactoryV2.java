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
package com.bah.lucene;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.store.Directory;

import com.bah.lucene.blockcache_v2.*;
import com.bah.lucene.blockcache_v2.BaseCache.STORE;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.Map.Entry;

import static com.bah.lucene.util.BlurConstants.*;

public class BlockCacheDirectoryFactoryV2 extends BlockCacheDirectoryFactory {

  private static final Log LOG = LogFactory.getLog(BlockCacheDirectoryFactoryV2.class);

  private final Cache _cache;

  public BlockCacheDirectoryFactoryV2(Configuration configuration, long totalNumberOfBytes) {

    final int fileBufferSizeInt = configuration.getInt(BLUR_SHARD_BLOCK_CACHE_V2_FILE_BUFFER_SIZE, 8192);
    LOG.info(MessageFormat.format("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_FILE_BUFFER_SIZE, fileBufferSizeInt));
    final int cacheBlockSizeInt = configuration.getInt(BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE, 8192);
    LOG.info(MessageFormat.format("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE, cacheBlockSizeInt));

    final Map<String,Integer> cacheBlockSizeMap = new HashMap<String, Integer>();
      for (Entry<String, String> prop : configuration) {
      String key = prop.getKey();
      if (key.startsWith(BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE_PREFIX)) {
        String value = prop.getValue();
        int cacheBlockSizeForFile = Integer.parseInt(value);
        String fieldType = key.substring(BLUR_SHARD_BLOCK_CACHE_V2_CACHE_BLOCK_SIZE_PREFIX.length());
        
        cacheBlockSizeMap.put(fieldType, cacheBlockSizeForFile);
        LOG.info(MessageFormat.format("{0}={1} for file type [{2}]", key, cacheBlockSizeForFile, fieldType));
      }
    }

    final STORE store = STORE.valueOf(configuration.get(BLUR_SHARD_BLOCK_CACHE_V2_STORE, OFF_HEAP));
    LOG.info(MessageFormat.format("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_STORE, store));

    final Set<String> cachingFileExtensionsForRead = getSet(configuration.get(
        BLUR_SHARD_BLOCK_CACHE_V2_READ_CACHE_EXT, DEFAULT_VALUE));
    LOG.info(MessageFormat.format("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_READ_CACHE_EXT, cachingFileExtensionsForRead));
    
    final Set<String> nonCachingFileExtensionsForRead = getSet(configuration.get(
        BLUR_SHARD_BLOCK_CACHE_V2_READ_NOCACHE_EXT, DEFAULT_VALUE));
    LOG.info(MessageFormat.format("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_READ_NOCACHE_EXT, nonCachingFileExtensionsForRead));
    
    final boolean defaultReadCaching = configuration.getBoolean(BLUR_SHARD_BLOCK_CACHE_V2_READ_DEFAULT, true);
    LOG.info(MessageFormat.format("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_READ_DEFAULT, defaultReadCaching));

    final Set<String> cachingFileExtensionsForWrite = getSet(configuration.get(
        BLUR_SHARD_BLOCK_CACHE_V2_WRITE_CACHE_EXT, DEFAULT_VALUE));
    LOG.info(MessageFormat.format("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_WRITE_CACHE_EXT, cachingFileExtensionsForWrite));
    
    final Set<String> nonCachingFileExtensionsForWrite = getSet(configuration.get(
        BLUR_SHARD_BLOCK_CACHE_V2_WRITE_NOCACHE_EXT, DEFAULT_VALUE));
    LOG.info(MessageFormat.format("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_WRITE_NOCACHE_EXT, nonCachingFileExtensionsForWrite));
    
    final boolean defaultWriteCaching = configuration.getBoolean(BLUR_SHARD_BLOCK_CACHE_V2_WRITE_DEFAULT, true);
    LOG.info(MessageFormat.format("{0}={1}", BLUR_SHARD_BLOCK_CACHE_V2_WRITE_DEFAULT, defaultWriteCaching));
    

    Size fileBufferSize = new Size() {
      @Override
      public int getSize(CacheDirectory directory, String fileName) {
        return fileBufferSizeInt;
      }
    };

    Size cacheBlockSize = new Size() {
      @Override
      public int getSize(CacheDirectory directory, String fileName) {
        String ext = getExt(fileName);
        Integer size = cacheBlockSizeMap.get(ext);
        if (size != null) {
          return size;
        }
        return cacheBlockSizeInt;
      }
    };

    FileNameFilter readFilter = new FileNameFilter() {
      @Override
      public boolean accept(CacheDirectory directory, String fileName) {
        String ext = getExt(fileName);
        if (cachingFileExtensionsForRead.contains(ext)) {
          return true;
        } else if (nonCachingFileExtensionsForRead.contains(ext)) {
          return false;
        }
        return defaultReadCaching;
      }
    };

    FileNameFilter writeFilter = new FileNameFilter() {
      @Override
      public boolean accept(CacheDirectory directory, String fileName) {
        String ext = getExt(fileName);
        if (cachingFileExtensionsForWrite.contains(ext)) {
          return true;
        } else if (nonCachingFileExtensionsForWrite.contains(ext)) {
          return false;
        }
        return defaultWriteCaching;
      }
    };

    Quiet quiet = new Quiet() {
      @Override
      public boolean shouldBeQuiet(CacheDirectory directory, String fileName) {
        Thread thread = Thread.currentThread();
        String name = thread.getName();
        if (name.startsWith(SHARED_MERGE_SCHEDULER)) {
          return true;
        }
        return false;
      }
    };

    _cache = new BaseCache(totalNumberOfBytes, fileBufferSize, cacheBlockSize, readFilter, writeFilter, quiet, store);
  }

  private Set<String> getSet(String value) {
    String[] split = value.split(",");
    return new HashSet<String>(Arrays.asList(split));
  }

  @Override
  public Directory newDirectory(String table, String shard, Directory directory, Set<String> tableBlockCacheFileTypes)
      throws IOException {
    return new CacheDirectory(table, shard, directory, _cache, tableBlockCacheFileTypes);
  }

  private static String getExt(String fileName) {
    int indexOf = fileName.lastIndexOf('.');
    if (indexOf < 0) {
      return DEFAULT_VALUE;
    }
    return fileName.substring(indexOf + 1);
  }

}
