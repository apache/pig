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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.util.UriUtil;

import java.io.IOException;

/**
 * Class that computes the size of output for file-based systems.
 */
public class FileBasedOutputSizeReader implements PigStatsOutputSizeReader {

    private static final Log log = LogFactory.getLog(FileBasedOutputSizeReader.class);

    /** 
     * Returns whether the given POStore is supported by this output size reader
     * or not. We check whether the uri scheme of output file is one of hdfs,
     * local, and s3.
     * @param sto POStore
     */
    @Override
    public boolean supports(POStore sto) {
        return UriUtil.isHDFSFileOrLocalOrS3N(getLocationUri(sto));
    }

    /**
     * Returns the total size of output files in bytes
     * @param sto POStore
     * @param conf configuration
     */
    @Override
    public long getOutputSize(POStore sto, Configuration conf) throws IOException {
        if (!supports(sto)) {
            log.warn("'" + sto.getStoreFunc().getClass().getName()
                    + "' is not supported by " + getClass().getName());
            return -1;
        }

        long bytes = 0;
        Path p = new Path(getLocationUri(sto));
        FileSystem fs = p.getFileSystem(conf);
        FileStatus[] lst = fs.listStatus(p);
        if (lst != null) {
            for (FileStatus status : lst) {
                bytes += status.getLen();
            }
        }

        return bytes;
    }

    /**
     * Returns the uri of output file in string
     * @param sto POStore
     */
    private static String getLocationUri(POStore sto) {
        return sto.getSFile().getFileName();
    }
}
