package com.bah.lucene.hdfs;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.lucene.store.*;

import com.bah.lucene.blockcache.LastModified;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class HdfsDirectory extends BaseDirectory implements LastModified {

  private static final Log LOG = LogFactory.getLog(HdfsDirectory.class);

  public static AtomicInteger fetchImpl = new AtomicInteger(3);

  // static {
  // Thread thread = new Thread(new Runnable() {
  // @Override
  // public void run() {
  // while (true) {
  // File file = new File("/tmp/fetch.impl");
  // if (file.exists()) {
  // try {
  // BufferedReader reader = new BufferedReader(new InputStreamReader(new
  // FileInputStream(file)));
  // String line = reader.readLine();
  // String trim = line.trim();
  // int i = Integer.parseInt(trim);
  // if (i != fetchImpl.get()) {
  // LOG.info("Changing fetch impl [" + i + "]");
  // fetchImpl.set(i);
  // }
  // reader.close();
  // } catch (Exception e) {
  // LOG.error("Unknown error", e);
  // }
  // }
  // try {
  // Thread.sleep(5000);
  // } catch (InterruptedException e) {
  // return;
  // }
  // }
  // }
  // });
  // thread.setDaemon(true);
  // thread.start();
  // }

  protected final Path _path;
  protected final FileSystem _fileSystem;

  public HdfsDirectory(Configuration configuration, Path path) throws IOException {
    this._path = path;
    _fileSystem = path.getFileSystem(configuration);
    _fileSystem.mkdirs(path);
    setLockFactory(NoLockFactory.getNoLockFactory());
  }

  @Override
  public String toString() {
    return "HdfsDirectory path=[" + _path + "]";
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    LOG.debug(MessageFormat.format("createOutput [{0}] [{1}] [{2}]", name, context, _path));
    if (fileExists(name)) {
      throw new IOException("File [" + name + "] already exists found.");
    }
    final FSDataOutputStream outputStream = openForOutput(name);
    return new OutputStreamIndexOutput(outputStream, 16384);
  }

  protected FSDataOutputStream openForOutput(String name) throws IOException {
    return _fileSystem.create(getPath(name));
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    LOG.debug(MessageFormat.format("openInput [{0}] [{1}] [{2}]", name, context, _path));
    if (!fileExists(name)) {
      throw new FileNotFoundException("File [" + name + "] not found.");
    }
    FSDataInputStream inputStream = openForInput(name);
    long fileLength = fileLength(name);
    return new HdfsIndexInput(name, inputStream, fileLength, fetchImpl.get());
  }

  protected FSDataInputStream openForInput(String name) throws IOException {
    return _fileSystem.open(getPath(name));
  }

  @Override
  public String[] listAll() throws IOException {
    LOG.debug(MessageFormat.format("listAll [{0}]", _path));
    FileStatus[] files = _fileSystem.listStatus(_path, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        try {
          return _fileSystem.isFile(path);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    String[] result = new String[files.length];
    for (int i = 0; i < result.length; i++) {
      result[i] = files[i].getPath().getName();
    }
    return result;
  }

  @Override
  public boolean fileExists(String name) throws IOException {
    LOG.debug(MessageFormat.format("fileExists [{0}] [{1}]", name, _path));
    return exists(name);
  }

  protected boolean exists(String name) throws IOException {
    return _fileSystem.exists(getPath(name));
  }

  @Override
  public void deleteFile(String name) throws IOException {
    LOG.debug(MessageFormat.format("deleteFile [{0}] [{1}]", name, _path));
    if (fileExists(name)) {
      delete(name);
    } else {
      throw new FileNotFoundException("File [" + name + "] not found");
    }
  }

  protected void delete(String name) throws IOException {
    _fileSystem.delete(getPath(name), true);
  }

  @Override
  public long fileLength(String name) throws IOException {
    LOG.debug(MessageFormat.format("fileLength [{0}] [{1}]", name, _path));
    return length(name);
  }

  protected long length(String name) throws IOException {
    return _fileSystem.getFileStatus(getPath(name)).getLen();
  }

  @Override
  public void sync(Collection<String> names) throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  public Path getPath() {
    return _path;
  }

  private Path getPath(String name) {
    return new Path(_path, name);
  }

  public long getFileModified(String name) throws IOException {
    if (!fileExists(name)) {
      throw new FileNotFoundException("File [" + name + "] not found");
    }
    return fileModified(name);
  }

  protected long fileModified(String name) throws IOException {
    return _fileSystem.getFileStatus(getPath(name)).getModificationTime();
  }

  @Override
  public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
    LOG.warn(MessageFormat.format("DANGEROUS copy [{0}] [{1}] [{2}] [{3}] [{4}]", to, src, dest, context, _path));
    if (to instanceof DirectoryDecorator) {
      copy(((DirectoryDecorator) to).getOriginalDirectory(), src, dest, context);
    } else if (to instanceof HdfsDirectory) {
      if (quickMove(to, src, dest, context)) {
        return;
      }
    } else {
      slowCopy(to, src, dest, context);
      
    }
  }

  protected void slowCopy(Directory to, String src, String dest, IOContext context) throws IOException {
    super.copy(to, src, dest, context);
  }

  private boolean quickMove(Directory to, String src, String dest, IOContext context) throws IOException {
    HdfsDirectory simpleTo = (HdfsDirectory) to;
    if (ifSameCluster(simpleTo, this)) {
      Path newDest = simpleTo.getPath(dest);
      Path oldSrc = getPath(src);
      return _fileSystem.rename(oldSrc, newDest);
    }
    return false;
  }

  private boolean ifSameCluster(HdfsDirectory dest, HdfsDirectory src) {
    // @TODO
    return true;
  }

}
