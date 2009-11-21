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

package org.apache.hadoop.zebra.io;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.zebra.tfile.TFile;
import org.apache.hadoop.zebra.tfile.MetaBlockAlreadyExists;
import org.apache.hadoop.zebra.tfile.MetaBlockDoesNotExist;


/**
 * simple named meta block management.
 * 
 * This implementation uses TFile as the storage layer.
 * 
 * TODO: Use an alternative design to allow writing of meta blocks from
 * multiple processes with DFS append feature.
 */
class MetaFile {
  // MetaFile is just a namespace object.
  private MetaFile() {
    // no-op
  }

  /**
   * Create a MetaFile writer.
   * 
   * @param path
   *          path to the meta file.
   * @param conf
   *          Configuration object
   * @return a MataFile.Writer object
   * @throws IOException
   *           upon error
   * 
   *           The implementation does not always hold a reference to the
   *           underlying file. In fact, the file is not created until the first
   *           time a meta block is added. For this implementation, we use TFile
   *           as storage backend, and only one process will ever succeed in
   *           creating the file and adding meta blocks. So it is possible that
   *           we successfully open the Writer initially, but receive a
   *           file-already-exist error later.
   */
  static Writer createWriter(Path path, Configuration conf)
      throws IOException {
    return new Writer(path, conf);
  }

  /**
   * Create a MetaFile reader.
   * 
   * @param path
   *          path to the meta file.
   * @param conf
   *          Configuration object
   * @return a MataFile.Reader object
   * @throws IOException
   *           upon error
   * 
   *           The implementation does not always hold a reference to the
   *           underlying file. In fact, the file is opened when the first time
   *           a meta block is requested. So it is possible that we successfully
   *           open the reader, but receive a file-not-exist error later.
   */
  static Reader createReader(Path path, Configuration conf)
      throws IOException {
    return new Reader(path, conf);
  }

  /**
   * Reader
   */
  static class Reader implements Closeable {
    private Path path;
    private Configuration conf;
    private FSDataInputStream fsdis = null;
    private TFile.Reader metaFile = null;

    /**
     * Remember the settings. Creation of TFile reader is lazy.
     * 
     * @param path
     * @param conf
     */
    Reader(Path path, Configuration conf) {
      this.path = path;
      this.conf = conf;
    }

    private synchronized void checkFile() throws IOException {
      if (metaFile != null) return;
      FileSystem fs = path.getFileSystem(conf);
      fsdis = fs.open(path);
      metaFile = new TFile.Reader(fsdis, fs.getFileStatus(path).getLen(), conf);
    }
    
    DataInputStream getMetaBlock(String name)
        throws MetaBlockDoesNotExist, IOException {
      checkFile();
      return metaFile.getMetaBlock(name);
    }

    public void close() throws IOException {
      try {
        if (metaFile != null) {
          metaFile.close();
          metaFile = null;
        }
        if (fsdis != null) {
          fsdis.close();
          fsdis = null;
        }
      } finally {
        if (metaFile != null) {
          try {
            metaFile.close();
          } catch (Exception e) {
            // no-op
          }
          metaFile = null;
        }
        
        if (fsdis != null) {
          try {
            fsdis.close();
          } catch (Exception e) {
            // no-op
          }
          fsdis = null;
        }
      }
    }
  }

  /**
   * Writer
   */
  static class Writer implements Closeable {
    private Path path;
    private Configuration conf;
    private FSDataOutputStream fsdos = null;
    private TFile.Writer metaFile = null;

    // Creation of TFile writer is lazy.
    Writer(Path path, Configuration conf) {
      this.path = path;
      this.conf = conf;
    }
    
    /**
     * Actually opening the file if not opened yet.
     * 
     * @throws IOException
     */
    private synchronized void checkFile() throws IOException {
      if (metaFile != null) return;

      FileSystem fs = path.getFileSystem(conf);
      // Throw an exception if the meta file already exists.
      fsdos = fs.create(path, false);   
      // TODO Move getMinBlockSize to BasicTable
      // TODO move getCompression to BasicTable
      metaFile =
          new TFile.Writer(fsdos, ColumnGroup.getMinBlockSize(conf),
              ColumnGroup.getCompression(conf), null, conf);
    }

    /**
     * Close the Writer if it is opened. Generally, finish() should allow future
     * writer to append more data. But in this implementation, we are not able
     * to achieve this.
     * 
     * @throws IOException
     */
    void finish() throws IOException {
      close();
    }

    /**
     * Close the Writer if it is opened.
     * 
     * @throws IOException
     */
    public void close() throws IOException {
      try {
        if (metaFile != null) {
          metaFile.close();
          metaFile = null;
        }
        if (fsdos != null) {
          fsdos.close();
          fsdos = null;
        }
      } finally {
        if (metaFile != null) {
          try {
            metaFile.close();
          } catch (Exception e) {
            // no-op
          }
          metaFile = null;
        }
        
        if (fsdos != null) {
          try {
            fsdos.close();
          } catch (Exception e) {
            // no-op
          }
          fsdos = null;
        }
      }
    }

    /**
     * Obtain an output stream for creating a Meta Block with the specific
     * name. The first time it is called, the meta file will be created.
     * 
     * @param name
     *          The name of the Meta Block
     * @return The output stream. Close the stream to conclude the writing.
     * @throws IOException
     * @throws MetaBlockAlreadyExists
     */
    public DataOutputStream createMetaBlock(String name)
        throws MetaBlockAlreadyExists, IOException {
      checkFile();
      return metaFile.prepareMetaBlock(name);
    }
  }
}
