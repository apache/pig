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

package org.apache.pig.tools.pigstats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * This class encapsulates the runtime statistics of a user specified input.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class InputStats {

    private String name;
    private String location;
    private long bytes;
    private long records;

    private boolean success;

    public static enum INPUT_TYPE { regular, sampler, indexer, side };

    private INPUT_TYPE type = INPUT_TYPE.regular;

    private Configuration conf;

    public InputStats(String location, long bytes, long records, boolean success) {
        this.location = location;
        this.bytes = bytes;
        this.records = records;
        this.success = success;
        try {
            this.name = new Path(location).getName();
        } catch (Exception e) {
            // location is a mal formatted URL
            this.name = location;
        }
    }

    public String getName() {
        return name;
    }

    public String getLocation() {
        return location;
    }

    public long getBytes() {
        return bytes;
    }

    public long getNumberRecords() {
        return records;
    }

    public boolean isSuccessful() {
        return success;
    }

    public Configuration getConf() {
        return conf;
    }

    public INPUT_TYPE getInputType() {
        return type;
    }

    public String getDisplayString(boolean local) {
        StringBuilder sb = new StringBuilder();
        if (success) {
            sb.append("Successfully ");
            if (type == INPUT_TYPE.sampler) {
                sb.append("sampled ");
            } else if (type == INPUT_TYPE.indexer) {
                sb.append("indexed ");
            } else {
                sb.append("read ");
            }

            if (!local && records >= 0) {
                sb.append(records).append(" records ");
            } else {
                sb.append("records ");
            }
            if (bytes > 0) {
                sb.append("(").append(bytes).append(" bytes) ");
            }
            sb.append("from: \"").append(location).append("\"");
            if (type == INPUT_TYPE.side) {
                sb.append(" as side file");
            }
            sb.append("\n");
        } else {
            sb.append("Failed to read data from \"").append(location)
                    .append("\"\n");
        }
        return sb.toString();
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public void markSampleInput() {
        type = INPUT_TYPE.sampler;
    }

    public void markIndexerInput() {
        type = INPUT_TYPE.indexer;
    }

    public void markSideFileInput() {
        type = INPUT_TYPE.side;
    }
}
