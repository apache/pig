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

package org.apache.hadoop.owl.testdriver;

import java.util.List;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.pig.data.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class SampleDriverUtil {

    public static void createBasicTable(OwlSchema schema, String keyPrefix, int numPartFiles,
            int numRows, Path path) throws Exception{
        SampleDataGenerator genSpec = new SampleDataGenerator(schema, keyPrefix, numPartFiles, numRows, path);
        SampleInputStorageDriverImpl.seededData.put(path,genSpec);
    }

    public static void createBasicTable(OwlSchema schema, String keyPrefix, int numPartFiles,
            List<Tuple> tuples, Path path) throws Exception {
        SampleDataGenerator genSpec = new SampleDataGenerator(schema, keyPrefix, numPartFiles, tuples, path);
        SampleInputStorageDriverImpl.seededData.put(path,genSpec);
    }

    public static void dropData(Path path) throws IOException {
        SampleInputStorageDriverImpl.seededData.remove(path);

        Configuration conf = new Configuration();
        Path rootPath = new Path("test");
        FileSystem fs = rootPath.getFileSystem(conf);

        if (fs.exists(path)) {
            fs.delete(path,true); // Recursive delete, better be sure you really did want to drop that path.
        }

    }



}

