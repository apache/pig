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

package org.apache.pig.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.builtin.PigStorage;

public class PigStorageWithStatistics extends PigStorage {
    private String loc = null;

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        super.setLocation(location, job);
        loc = location;
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        ResourceStatistics stats = new ResourceStatistics();
        stats.setSizeInBytes(getInputSizeInBytes());
        return stats;
    }
    
    private Long getInputSizeInBytes() throws IOException {
        if (loc == null) {
            return 0L;
        }

        long inputBytes = 0L;
        for (String location : getPathStrings(loc)) {
            Path path = new Path(location);
            FileSystem fs = path.getFileSystem(new Configuration());
            FileStatus[] status = fs.globStatus(path);
            if (status != null) {
                for (FileStatus s : status) {
                    inputBytes += MapRedUtil.getPathLength(fs, s);
                }
            }
        }
        return inputBytes;
    }
}
