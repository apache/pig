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

package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;

public class PigSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {

    /*
     * This is to support multi-level/recursive directory listing until 
     * MAPREDUCE-1577 is fixed.
     */
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {        
        Path[] dirs = FileInputFormat.getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }
        List<FileStatus> files = new ArrayList<FileStatus>();
        for (int i=0; i<dirs.length; ++i) {
            Path p = dirs[i];
            FileSystem fs = p.getFileSystem(job.getConfiguration()); 
            FileStatus[] matches = fs.globStatus(p, hiddenFileFilter);
            if (matches == null) {
                throw new IOException("Input path does not exist: " + p);
            } else if (matches.length == 0) {
                throw new IOException("Input Pattern " + p + " matches 0 files");
            } else {
                for (FileStatus globStat: matches) {
                    files.add(globStat);
                }
            }
        }
        return MapRedUtil.getAllFileRecursively(files, job.getConfiguration());        
    }

    private static final PathFilter hiddenFileFilter = new PathFilter(){
        public boolean accept(Path p){
            String name = p.getName(); 
            return !name.startsWith("_") && !name.startsWith("."); 
        }
    };   
}
