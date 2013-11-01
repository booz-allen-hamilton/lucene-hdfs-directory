package org.apache.blur.store.buffer;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.MessageFormat;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class BufferStore {
  
  public static final int _1024_DEFAULT_SIZE = 8192;
  public static final int _8192_DEFAULT_SIZE = 8192;

  private static final Log LOG = LogFactory.getLog(BufferStore.class);

  private static BlockingQueue<byte[]> _1024;
  private static BlockingQueue<byte[]> _8192;

  private volatile static boolean setup = false;

  public static void init(int _1KSize, int _8KSize) {
    if (!setup) {
      LOG.info(MessageFormat.format("Initializing the 1024 buffers with [{0}] buffers.", _1KSize));
      _1024 = setupBuffers(1024, _1KSize);
      LOG.info(MessageFormat.format("Initializing the 8192 buffers with [{0}] buffers.", _8KSize));
      _8192 = setupBuffers(8192, _8KSize);
      setup = true;
    }
  }

  private static BlockingQueue<byte[]> setupBuffers(int bufferSize, int count) {
    BlockingQueue<byte[]> queue = new ArrayBlockingQueue<byte[]>(count);
    for (int i = 0; i < count; i++) {
      queue.add(new byte[bufferSize]);
    }
    return queue;
  }

  public static byte[] takeBuffer(int bufferSize) {
    if(_1024 == null || _8192 == null) {
      init(_1024_DEFAULT_SIZE, _8192_DEFAULT_SIZE);
    } 
    switch (bufferSize) {
    case 1024:
      return newBuffer1024(_1024.poll());
    case 8192:
      return newBuffer8192(_8192.poll());
    default:
      return newBuffer(bufferSize);
    }
  }

  public static void putBuffer(byte[] buffer) {
    if (buffer == null) {
      return;
    }
    int bufferSize = buffer.length;
    switch (bufferSize) {
    case 1024:
      _1024.offer(buffer);
      return;
    case 8192:
      _8192.offer(buffer);
      return;
    }
  }

  private static byte[] newBuffer1024(byte[] buf) {
    if (buf != null) {
      return buf;
    }
    return new byte[1024];
  }

  private static byte[] newBuffer8192(byte[] buf) {
    if (buf != null) {
      return buf;
    }
    return new byte[8192];
  }

  private static byte[] newBuffer(int size) {
    return new byte[size];
  }
}
