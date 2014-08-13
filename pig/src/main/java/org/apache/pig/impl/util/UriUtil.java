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
package org.apache.pig.impl.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;

public class UriUtil {
    public static boolean isHDFSFile(String uri){
        if(uri == null)
            return false;
        if (uri.startsWith("/") || uri.startsWith("hdfs:") || uri.startsWith("viewfs:") ||
                uri.startsWith("hftp:") || uri.startsWith("webhdfs:")) {
            return true;
        }
        return false;
    }

    public static boolean isHDFSFileOrLocalOrS3N(String uri, Configuration conf){
        if(uri == null)
            return false;
        return HadoopShims.hasFileSystemImpl(new Path(uri), conf);
    }

}
