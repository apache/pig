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

package org.apache.pig;

/**
 * Container for static configuration strings, defaults, etc. This is intended just for keys that can
 * be set by users, not for keys that are generally used within pig.
 */
public class PigConfiguration {
    private PigConfiguration() {}

    /////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////       COMMAND LINE KEYS       /////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////

    /**
     * Controls the fraction of total memory that is allowed to be used by
     * cached bags. Default is 0.2.
     */
    public static final String PROP_CACHEDBAG_MEMUSAGE = "pig.cachedbag.memusage";

    /**
     * Controls whether partial aggregation is turned on
     */
    public static final String PROP_EXEC_MAP_PARTAGG = "pig.exec.mapPartAgg";

    /**
     * Controls the minimum reduction in-mapper Partial Aggregation should achieve in order
     * to stay on. If after a period of observation this reduction is not achieved,
     * in-mapper aggregation will be turned off and a message logged to that effect.
     */
    public static final String PARTAGG_MINREDUCTION = "pig.exec.mapPartAgg.minReduction";

    /**
     * Controls whether execution time of Pig UDFs should be tracked.
     * This feature uses counters; use judiciously.
     */
    public static final String TIME_UDFS = "pig.udf.profile";
    public static final String TIME_UDFS_FREQUENCY = "pig.udf.profile.frequency";

    /**
     * This key must be set to true by the user for code generation to be used.
     * In the future, it may be turned on by default (at least in certain cases),
     * but for now it is too experimental.
     */
    public static final String SHOULD_USE_SCHEMA_TUPLE = "pig.schematuple";

    public static final String SCHEMA_TUPLE_SHOULD_USE_IN_UDF = "pig.schematuple.udf";

    public static final String SCHEMA_TUPLE_SHOULD_USE_IN_FOREACH = "pig.schematuple.foreach";

    public static final String SCHEMA_TUPLE_SHOULD_USE_IN_FRJOIN = "pig.schematuple.fr_join";

    public static final String SCHEMA_TUPLE_SHOULD_USE_IN_MERGEJOIN = "pig.schematuple.merge_join";

    public static final String SCHEMA_TUPLE_SHOULD_ALLOW_FORCE = "pig.schematuple.force";

    /**
     * This key is used to enable multiquery optimization.
     */
    public static final String OPT_MULTIQUERY = "opt.multiquery";

    /**
     * This key is used to enable accumulator optimization.
     */
    public static final String OPT_ACCUMULATOR = "opt.accumulator";


    /**
     * This key is used to configure auto parallelism in tez. Default is true.
     */
    public static final String TEZ_AUTO_PARALLELISM = "pig.tez.auto.parallelism";

    /**
     * This key is used to enable union optimization.
     */
    public static final String TEZ_OPT_UNION = "pig.tez.opt.union";

    /**
     * This key is used to define whether to reuse AM in Tez jobs.
     */
    public static final String TEZ_SESSION_REUSE = "pig.tez.session.reuse";

    /**
     * This key is used to configure the interval of dag status report in seconds.
     */
    public static final String TEZ_DAG_STATUS_REPORT_INTERVAL = "pig.tez.dag.status.report.interval";

    /**
     * Turns off use of combiners in MapReduce jobs produced by Pig.
     */
    public static final String PROP_NO_COMBINER = "pig.exec.nocombiner";

    /**
     * This key can be used to defined what properties will be set in the streaming environment.
     * Just set this property to a comma-delimited list of properties to set, and those properties
     * will be set in the environment.
     */
    public static final String PIG_STREAMING_ENVIRONMENT = "pig.streaming.environment";

    /**
     * This key is used to define the default load func. Pig will fallback on PigStorage
     * as default in case this is undefined.
     */
    public static final String PIG_DEFAULT_LOAD_FUNC = "pig.default.load.func";

    /**
     * This key is used to define the default store func. Pig will fallback on PigStorage
     * as default in case this is undefined.
     */
    public static final String PIG_DEFAULT_STORE_FUNC = "pig.default.store.func";

    /**
     * This key is used to define whether to support recovery to handle the
     * application master getting restarted.
     */
    public static final String PIG_OUTPUT_COMMITTER_RECOVERY = "pig.output.committer.recovery.support";

    /**
     * This key is used to turn off the inclusion of settings in the jobs.
     */
    public static final String INSERT_ENABLED = "pig.script.info.enabled";

    /**
     * Controls the size of Pig script stored in job xml.
     */
    public static final String MAX_SCRIPT_SIZE = "pig.script.max.size";

    /**
     * This key is used to define whether to have intermediate file compressed
     */
    public static final String PIG_ENABLE_TEMP_FILE_COMPRESSION = "pig.tmpfilecompression";

    /**
     * This key is used to set the storage type used by intermediate file storage
     * If pig.tmpfilecompression, default storage used is TFileStorage.
     * This can be overriden to use SequenceFileInterStorage by setting following property to "seqfile".
     */
    public static final String PIG_TEMP_FILE_COMPRESSION_STORAGE = "pig.tmpfilecompression.storage";

    /**
     * Compression codec used by intermediate storage
     * TFileStorage only support gzip and lzo.
     */
    public static final String PIG_TEMP_FILE_COMPRESSION_CODEC = "pig.tmpfilecompression.codec";

    /**
     * This key is used to define whether to delete intermediate files of Hadoop jobs.
     */
    public static final String PIG_DELETE_TEMP_FILE = "pig.delete.temp.files";

    /**
     * For a given mean and a confidence, a sample rate is obtained from a poisson udf
     */
    public static final String SAMPLE_RATE = "pig.sksampler.samplerate";

    /**
     * % of memory available for the input data. This is currently equal to the
     * memory available for the skewed join
     */
    public static final String PERC_MEM_AVAIL = "pig.skewedjoin.reduce.memusage";

    /**
     * This key used to control the maximum size loaded into
     * the distributed cache when doing fragment-replicated join
     */
    public static final String PIG_JOIN_REPLICATED_MAX_BYTES = "pig.join.replicated.max.bytes";

    /**
     * Turns combine split files on or off
     */
    public static final String PIG_SPLIT_COMBINATION = "pig.splitCombination";

    /**
     * Whether turns combine split files off. This is for internal use only
     */
    public static final String PIG_NO_SPLIT_COMBINATION = "pig.noSplitCombination";

    /**
     * Specifies the size, in bytes, of data to be processed by a single map.
     * Smaller files are combined untill this size is reached.
     */
    public static final String PIG_MAX_COMBINED_SPLIT_SIZE = "pig.maxCombinedSplitSize";

    /**
     * This key controls whether secondary sort key is used for optimization in case
     * of nested distinct or sort
     */
    public static final String PIG_EXEC_NO_SECONDARY_KEY = "pig.exec.nosecondarykey";

    /**
     * This key used to control the sample size of RandomeSampleLoader for
     * order-by. The default value is 100 rows per task.
     */
    public static final String PIG_RANDOM_SAMPLER_SAMPLE_SIZE = "pig.random.sampler.sample.size";

    /**
     * This key is to turn on auto local mode feature
     */
    public static final String PIG_AUTO_LOCAL_ENABLED = "pig.auto.local.enabled";

    /**
     * Controls the max threshold size to convert jobs to run in local mode
     */
    public static final String PIG_AUTO_LOCAL_INPUT_MAXBYTES = "pig.auto.local.input.maxbytes";

    /**
     * This parameter enables/disables fetching. By default it is turned on.
     */
    public static final String OPT_FETCH = "opt.fetch";

    /**
     * This key is used to define whether PigOutputFormat will be wrapped with LazyOutputFormat
     * so that jobs won't write empty part files if no output is generated
     */
    public static final String PIG_OUTPUT_LAZY = "pig.output.lazy";

    /**
     * Location where pig stores temporary files for job setup
     */
    public static final String PIG_TEMP_DIR = "pig.temp.dir";

    /**
     * This key is turn on the user level cache
     */
    public static final String PIG_USER_CACHE_ENABLED = "pig.user.cache.enabled";

    /**
     * Location where additional jars are cached for the user
     * Additional jar will be cached under PIG_USER_CACHE_LOCATION/${user.name}/.pigcache
     * and will be re-used across the jobs run by the user if the jar has not changed
     */
    public static final String PIG_USER_CACHE_LOCATION = "pig.user.cache.location";

    /**
     * Comma-delimited entries of commands/operators that must be disallowed.
     * This is a security feature to be used by administrators to block use of
     * commands by users. For eg, an admin might like to block all filesystem
     * commands and setting configs in pig script. In which case, the entry
     * would be "pig.blacklist=fs,set"
     */
    public static final String PIG_BLACKLIST = "pig.blacklist";

    /**
     * Comma-delimited entries of commands/operators that must be allowed. This
     * is a security feature to be used by administrators to block use of
     * commands by users that are not a part of the whitelist. For eg, an admin
     * might like to allow only LOAD, STORE, FILTER, GROUP in pig script. In
     * which case, the entry would be "pig.whitelist=load,store,filter,group"
     */
    public static final String PIG_WHITELIST = "pig.whitelist";

    /**
     * This key is used to turns off use of task reports in job statistics.
     */
    public static final String PIG_NO_TASK_REPORT = "pig.stats.notaskreport";
}
