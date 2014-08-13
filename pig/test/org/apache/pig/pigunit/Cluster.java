/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.apache.pig.pigunit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.Util;

/**
 * Encapsulates all the file system operations.
 *
 * <p>Mainly used for copying data to the test cluster.
 */
public class Cluster {
  private final Configuration configuration;

  public Cluster(PigContext context) {
    configuration = ConfigurationUtil.toConfiguration(context.getProperties());
  }

  public boolean exists(Path destination) throws IOException {
    FileSystem fs = destination.getFileSystem(configuration);
    return fs.exists(destination);
  }

  /**
   * If file size has changed, or if destination does not exist yet, copy it.
   *
   * @param local Path to the local file to copy to the cluster.
   * @param destination Destination path on the cluster.
   * @throws IOException If the copy failed.
   */
  public void update(Path local, Path destination) throws IOException {
    if (! exists(destination) || ! sameSize(local, destination)) {
      copyFromLocalFile(local, destination, true);
    }
  }

  public void copyFromLocalFile(Path local, Path destination) throws IOException {
    copyFromLocalFile(local, destination, true);
  }

  public void copyFromLocalFile(Path local, Path destination, boolean overwrite)
      throws IOException {
    FileSystem fs = local.getFileSystem(configuration);
    fs.copyFromLocalFile(false, overwrite, local, destination);
  }

  public void copyFromLocalFile(String[] content, String destination) throws IOException {
    copyFromLocalFile(content, destination, true);
  }

  public void copyFromLocalFile(String[] content, String destination, boolean overwrite)
      throws IOException {
    Path file = new Path(destination);
    FileSystem fs = file.getFileSystem(configuration);

    if (overwrite && fs.exists(file)) {
      fs.delete(file, true);
    }

    Util.createInputFile(fs, destination, content);
  }

  public void copyFromLocalFile(String[][] data) throws IOException {
    copyFromLocalFile(data, false);
  }

  public void copyFromLocalFile(String[][] data, boolean overwrite) throws IOException {
    for (int i = 0; i < data.length; i++) {
      copyFromLocalFile(new Path(data[i][0]), new Path(data[i][1]), overwrite);
    }
  }

  public FileStatus[] listStatus(Path path) throws IOException {
    FileSystem fs = path.getFileSystem(configuration);
    return fs.listStatus(path);
  }

  public boolean delete(Path path) throws IOException {
    FileSystem fs = path.getFileSystem(configuration);
    return fs.delete(path, true);
  }

  private boolean sameSize(Path local, Path destination) throws IOException {
    FileSystem fs1 = FileSystem.getLocal(configuration);
    FileSystem fs2 = destination.getFileSystem(configuration);

    return fs1.getFileStatus(local).getLen() == fs2.getFileStatus(destination).getLen();
  }
}
