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
package org.apache.pig.test;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.builtin.PigStorage;

public class PigTestLoader extends PigStorage {

    // This is to test PIG-1651: PIG class loading error
    private static boolean test = false;
    
    @Override
    public void setLocation(String location, Job job) throws IOException {
        super.setLocation(location, job);
        FileInputFormat.setInputPathFilter(job, TestPathFilter.class);
        test = true;
    }

    public static class TestPathFilter implements PathFilter {
        
        public TestPathFilter() {
            if (!test) throw new RuntimeException("Invalid static variable");
        }
        
        @Override
        public boolean accept(Path p) {
            String name = p.getName();            
            return !name.endsWith(".xml");
        }       
    }

}
