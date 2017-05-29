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

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.tools.pigstats.mapreduce.MRPigStatsUtil;
import org.apache.pig.tools.pigstats.mapreduce.SimplePigStats;

/**
 * A utility class for Pig Statistics
 */
public class PigStatsUtil {

    public static final String MAP_INPUT_RECORDS
            = "MAP_INPUT_RECORDS";
    public static final String MAP_OUTPUT_RECORDS
            = "MAP_OUTPUT_RECORDS";
    public static final String REDUCE_INPUT_RECORDS
            = "REDUCE_INPUT_RECORDS";
    public static final String REDUCE_OUTPUT_RECORDS
            = "REDUCE_OUTPUT_RECORDS";
    public static final String HDFS_BYTES_WRITTEN
            = "HDFS_BYTES_WRITTEN";
    public static final String HDFS_BYTES_READ
            = "HDFS_BYTES_READ";
    public static final String FILE_BYTES_WRITTEN
            = "FILE_BYTES_WRITTEN";
    public static final String FILE_BYTES_READ
            = "FILE_BYTES_READ";


    public static final String MULTI_INPUTS_RECORD_COUNTER
            = "Input records from ";
    public static final String MULTI_INPUTS_COUNTER_GROUP
            = "MultiInputCounters";
    public static final String MULTI_STORE_RECORD_COUNTER
            = "Output records in ";
    public static final String MULTI_STORE_COUNTER_GROUP
            = "MultiStoreCounters";

    /**
     * @deprecated use {@link org.apache.pig.tools.pigstats.mapreduce.MRPigStatsUtil#TASK_COUNTER_GROUP} instead.
     */
    @Deprecated
    public static final String TASK_COUNTER_GROUP
            = "org.apache.hadoop.mapred.Task$Counter";

    /**
     * @deprecated use {@link org.apache.pig.tools.pigstats.mapreduce.MRPigStatsUtil#FS_COUNTER_GROUP} instead.
     */
    @Deprecated
    public static final String FS_COUNTER_GROUP
            = MRPigStatsUtil.FS_COUNTER_GROUP;

    /**
     * Returns an empty PigStats object Use of this method is not advised as it
     * will return the MR execution engine version of PigStats by default, and
     * is not necessarily empty depending on the timing.
     *
     * @return an empty PigStats object
     */
    @Deprecated
    public static PigStats getEmptyPigStats() {
        return PigStats.start(new SimplePigStats());
    }

    /**
     * Returns the PigStats with the given return code
     *
     * @param code the return code
     * @return the PigStats with the given return code
     */
    public static PigStats getPigStats(int code) {
        PigStats ps = PigStats.get();
        if (ps == null) {
            ps = PigStats.start(new EmptyPigStats());
        }
        ps.setReturnCode(code);
        return ps;
    }

    public static void setErrorMessage(String msg) {
        PigStats ps = PigStats.get();
        if (ps == null) {
            ps = PigStats.start(new EmptyPigStats());
        }
        ps.setErrorMessage(msg);
    }

    public static void setErrorCode(int code) {
        PigStats ps = PigStats.get();
        if (ps == null) {
            ps = PigStats.start(new EmptyPigStats());
        }
        ps.setErrorCode(code);
    }

    public static void setErrorThrowable(Throwable t) {
        PigStats ps = PigStats.get();
        if (ps == null) {
            ps = PigStats.start(new EmptyPigStats());
        }
        ps.setErrorThrowable(t);
    }

    private static Pattern pattern = Pattern.compile("tmp(-)?[\\d]{1,10}$");

    public static boolean isTempFile(String fileName) {
        Matcher result = pattern.matcher(fileName);
        return result.find();
    }

    public static void setStatsMap(Map<String, List<PigStats>> statsMap) {
        PigStats.start(new EmbeddedPigStats(statsMap));
    }

    /**
     * Returns the counter name for the given input file name
     *
     * @param fname the input file name
     * @return the counter name
     */
    public static String getMultiInputsCounterName(String fname, int index) {
        String shortName = getShortName(fname);
        return (shortName == null) ? null
                : MULTI_INPUTS_RECORD_COUNTER + "_" + index + "_" + shortName;
    }

    /**
     * Returns the counter name for the given {@link POStore}
     *
     * @param store the POStore
     * @return the counter name
     */
    public static String getMultiStoreCounterName(POStore store) {
        String shortName = getShortName(store.getSFile().getFileName());
        return (shortName == null) ? null
                : MULTI_STORE_RECORD_COUNTER + "_" + store.getIndex() + "_" + shortName;
    }

    // Restrict total string size of a counter name to 64 characters.
    // Leave 24 characters for prefix string.
    private static final int COUNTER_NAME_LIMIT = 40;
    private static final String SEPARATOR = "/";
    private static final String SEMICOLON = ";";

    public static String getShortName(String uri) {
        int scolon = uri.indexOf(SEMICOLON);
        int slash;
        if (scolon!=-1) {
            slash = uri.lastIndexOf(SEPARATOR, scolon);
        } else {
            slash = uri.lastIndexOf(SEPARATOR);
        }
        String shortName = null;
        if (scolon==-1) {
            shortName = uri.substring(slash+1);
        }
        if (slash < scolon) {
            shortName = uri.substring(slash+1, scolon);
        }
        if (shortName != null && shortName.length() > COUNTER_NAME_LIMIT) {
            shortName = shortName.substring(shortName.length()
                    - COUNTER_NAME_LIMIT);
        }
        return shortName;
    }

}
