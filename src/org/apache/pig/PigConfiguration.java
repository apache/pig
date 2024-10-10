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

    // Pig runtime optimizations
    /**
     * This key is to turn on auto local mode feature
     */
    public static final String PIG_AUTO_LOCAL_ENABLED = "pig.auto.local.enabled";
    /**
     * Controls the max threshold size to convert jobs to run in local mode
     */
    public static final String PIG_AUTO_LOCAL_INPUT_MAXBYTES = "pig.auto.local.input.maxbytes";


    /**
     * Sets the length of record markers in binary files produces by Pig between jobs
     * The longer the byte sequence means less chance of collision with actual data,
     * shorter sequence means less overhead
     */
    public static final String PIG_INTERSTORAGE_SYNCMARKER_SIZE = "pig.interstorage.syncmarker.size";
    public static final int PIG_INTERSTORAGE_SYNCMARKER_SIZE_MAX = 16;
    public static final int PIG_INTERSTORAGE_SYNCMARKER_SIZE_DEFAULT = 10;
    public static final int PIG_INTERSTORAGE_SYNCMARKER_SIZE_MIN = 2;

    /**
     * Defines the interval (in bytes) when a sync marker should be written into the binary file
     */
    public static final String PIG_INTERSTORAGE_SYNCMARKER_INTERVAL = "pig.interstorage.syncmarker.interval";
    public static final long PIG_INTERSTORAGE_SYNCMARKER_INTERVAL_DEFAULT = 2000;


    /**
     * Boolean value used to enable or disable fetching without a mapreduce job for DUMP. True by default
     */
    public static final String PIG_OPT_FETCH = "opt.fetch";

    // Pig query planning and execution optimizations
    /**
     * Boolean value used to enable or disable multiquery optimization. True by default
     */
    public static final String PIG_OPT_MULTIQUERY = "opt.multiquery";

    /**
     * Boolean value used to enable or disable accumulator optimization. True by default
     */
    public static final String PIG_OPT_ACCUMULATOR = "opt.accumulator";
    public static final String PIG_ACCUMULATIVE_BATCHSIZE = "pig.accumulative.batchsize";

    /**
     * This key is used to enable or disable union optimization in tez. True by default
     */
    public static final String PIG_TEZ_OPT_UNION = "pig.tez.opt.union";
    /**
     * These keys are used to enable or disable tez union optimization for
     * specific StoreFuncs. Optimization should be turned off for those
     * StoreFuncs that hard code part file names and do not prefix file names
     * with mapreduce.output.basename configuration.
     *
     * If the StoreFuncs implement
     * {@link StoreFunc#supportsParallelWriteToStoreLocation()} and return true
     * or false then that is is used to turn on or off union optimization
     * respectively. These settings can be used for StoreFuncs that have not
     * implemented the API yet.
     */
    public static final String PIG_TEZ_OPT_UNION_SUPPORTED_STOREFUNCS = "pig.tez.opt.union.supported.storefuncs";
    public static final String PIG_TEZ_OPT_UNION_UNSUPPORTED_STOREFUNCS = "pig.tez.opt.union.unsupported.storefuncs";

    /**
     * Pig only reads once from datasource for LoadFuncs specified here during sort instead of
     * loading once for sampling and loading again for partitioning.
     * Used to avoid hitting external non-filesystem datasources like HBase and Accumulo twice.
     * Honored only by Pig on Tez now.
     */
    public static final String PIG_SORT_READONCE_LOADFUNCS = "pig.sort.readonce.loadfuncs";

    /**
     * Boolean value to enable or disable partial aggregation in map. Disabled by default
     */
    public static final String PIG_EXEC_MAP_PARTAGG = "pig.exec.mapPartAgg";
    /**
     * Controls the minimum reduction in-mapper Partial Aggregation should achieve in order
     * to stay on. If after a period of observation this reduction is not achieved,
     * in-mapper aggregation will be turned off and a message logged to that effect.
     */
    public static final String PIG_EXEC_MAP_PARTAGG_MINREDUCTION = "pig.exec.mapPartAgg.minReduction";

    /**
     * Boolean value to enable or disable use of combiners in MapReduce jobs. Enabled by default
     */
    public static final String PIG_EXEC_NO_COMBINER = "pig.exec.nocombiner";

    /**
     * Enable or disable use of combiners in reducer shuffle-merge phase in Tez.
     * Valid values are auto, true or false.
     * Default is auto which turns off combiner if bags are present in the combine plan
     */
    public static final String PIG_EXEC_NO_COMBINER_REDUCER = "pig.exec.nocombiner.reducer";

    /**
     * This key controls whether secondary sort key is used for optimization in case
     * of nested distinct or sort
     */
    public static final String PIG_EXEC_NO_SECONDARY_KEY = "pig.exec.nosecondarykey";

    // Pig memory usage control settings
    /**
     * Controls the fraction of total memory that is allowed to be used by
     * cached bags. Default is 0.2.
     */
    public static final String PIG_CACHEDBAG_MEMUSAGE = "pig.cachedbag.memusage";

    /**
     * % of memory available for the input data. This is currently equal to the
     * memory available for the skewed join
     */
    public static final String PIG_SKEWEDJOIN_REDUCE_MEMUSAGE = "pig.skewedjoin.reduce.memusage";

    /**
     * Memory available (in bytes) in reduce when calculating memory available for skewed join.
     * By default, it is set to Runtime.getRuntime().maxMemory(). Override it only
     * for debug purpose
     */
    public static final String PIG_SKEWEDJOIN_REDUCE_MEM = "pig.skewedjoin.reduce.mem";

    /**
     * Bloom join has two different kind of implementations.
     * <ul>
     * <li>map <br>
     * In each map, bloom filters are computed on the join keys partitioned by
     * the hashcode of the key with {@link #PIG_BLOOMJOIN_NUM_FILTERS} number of
     * partitions. Bloom filters from different maps are then combined in the
     * reducer producing one bloom filter per partition. This is efficient and
     * fast if there are smaller number of maps (<10) and the number of
     * distinct keys are not too high. It can be faster with larger number of
     * maps and even with bigger bloom vector sizes, but the amount of data
     * shuffled to the reducer for aggregation becomes huge making it
     * inefficient.</li>
     * <li>reduce <br>
     * Join keys are sent from the map to the reducer partitioned by hashcode of
     * the key with {@link #PIG_BLOOMJOIN_NUM_FILTERS} number of reducers. One
     * bloom filter is then created per partition. This is efficient for larger
     * datasets with lot of maps or very large
     * {@link #PIG_BLOOMJOIN_VECTORSIZE_BYTES}. In this case size of keys sent
     * to the reducer is smaller than sending bloom filters to reducer for
     * aggregation making it efficient.</li>
     * </ul>
     * Default value is map.
     */
    public static final String PIG_BLOOMJOIN_STRATEGY = "pig.bloomjoin.strategy";

    /**
     * The number of bloom filters that will be created.
     * Default is 1 for map strategy and 11 for reduce strategy.
     */
    public static final String PIG_BLOOMJOIN_NUM_FILTERS = "pig.bloomjoin.num.filters";

    /**
     * The size in bytes of the bit vector to be used for the bloom filter.
     * A bigger vector size will be needed when the number of distinct keys is higher.
     * Default value is 1048576 (1MB).
     */
    public static final String PIG_BLOOMJOIN_VECTORSIZE_BYTES = "pig.bloomjoin.vectorsize.bytes";

    /**
     * The type of hash function to use. Valid values are jenkins and murmur.
     * Default is murmur.
     */
    public static final String PIG_BLOOMJOIN_HASH_TYPE = "pig.bloomjoin.hash.type";

    /**
     * The number of hash functions to be used in bloom computation. It determines the probability of false positives.
     * Higher the number lower the false positives. Too high a value can increase the cpu time.
     * Default value is 3.
     */
    public static final String PIG_BLOOMJOIN_HASH_FUNCTIONS = "pig.bloomjoin.hash.functions";

    /**
     * To turn off combiner when most of the keys are unique.
     */
    public static final String PIG_BLOOMJOIN_NOCOMBINER = "pig.bloomjoin.nocombiner";
    /**
     * This key used to control the maximum size loaded into
     * the distributed cache when doing fragment-replicated join
     */
    public static final String PIG_JOIN_REPLICATED_MAX_BYTES = "pig.join.replicated.max.bytes";

    // Pig cached bag type settings
    /**
     * Configurations for specifying alternate implementations for cached bags. Rarely used
     */
    public static final String PIG_CACHEDBAG_TYPE = "pig.cachedbag.type";
    public static final String PIG_CACHEDBAG_DISTINCT_TYPE = "pig.cachedbag.distinct.type";
    public static final String PIG_CACHEDBAG_SORT_TYPE = "pig.cachedbag.sort.type";

    // Pig reducer parallelism estimation settings
    public static final String PIG_EXEC_REDUCER_ESTIMATOR = "pig.exec.reducer.estimator";
    public static final String PIG_EXEC_REDUCER_ESTIMATOR_CONSTRUCTOR_ARG_KEY =  "pig.exec.reducer.estimator.arg";
    /**
     * This key is used to configure auto parallelism in tez. Default is true.
     */
    public static final String PIG_TEZ_AUTO_PARALLELISM = "pig.tez.auto.parallelism";
    /**
     * This key is used to configure grace parallelism in tez. Default is true.
     */
    public static final String PIG_TEZ_GRACE_PARALLELISM = "pig.tez.grace.parallelism";
    /**
     * This key is used to turn off dag recovery if there is auto parallelism.
     * Default is false. Useful when running with Tez versions before Tez 0.8
     * which have issues with auto parallelism during DAG recovery.
     */
    public static final String PIG_TEZ_AUTO_PARALLELISM_DISABLE_DAG_RECOVERY = "pig.tez.auto.parallelism.disable.dag.recovery";

    /**
     * This key is used to configure compression for the pig input splits which
     * are not FileSplit. Default is false
     */
    public static final String PIG_COMPRESS_INPUT_SPLITS = "pig.compress.input.splits";
    public static final boolean PIG_COMPRESS_INPUT_SPLITS_DEFAULT = false;

    /**
     * Serialize input splits to disk if the input splits size exceeds a
     * threshold to avoid hitting default RPC transfer size limit of 64MB.
     * Default is 33554432 (32MB)
     */
    public static final String PIG_TEZ_INPUT_SPLITS_MEM_THRESHOLD = "pig.tez.input.splits.mem.threshold";
    public static final int PIG_TEZ_INPUT_SPLITS_MEM_THRESHOLD_DEFAULT = 33554432;

    // Pig UDF profiling settings
    /**
     * Controls whether execution time of Pig UDFs should be tracked.
     * This feature uses counters; use judiciously.
     */
    public static final String PIG_UDF_PROFILE = "pig.udf.profile";
    public static final String PIG_UDF_PROFILE_FREQUENCY = "pig.udf.profile.frequency";

    // Pig schema tuple settings
    /**
     * This key must be set to true by the user for code generation to be used.
     * In the future, it may be turned on by default (at least in certain cases),
     * but for now it is too experimental.
     */
    public static final String PIG_SCHEMA_TUPLE_ENABLED = "pig.schematuple";

    public static final String PIG_SCHEMA_TUPLE_USE_IN_UDF = "pig.schematuple.udf";

    public static final String PIG_SCHEMA_TUPLE_USE_IN_FOREACH = "pig.schematuple.foreach";

    public static final String PIG_SCHEMA_TUPLE_USE_IN_FRJOIN = "pig.schematuple.fr_join";

    public static final String PIG_SCHEMA_TUPLE_USE_IN_MERGEJOIN = "pig.schematuple.merge_join";

    public static final String PIG_SCHEMA_TUPLE_ALLOW_FORCE = "pig.schematuple.force";


    // Pig Streaming settings
    /**
     * This key can be used to defined what properties will be set in the streaming environment.
     * Just set this property to a comma-delimited list of properties to set, and those properties
     * will be set in the environment.
     */
    public static final String PIG_STREAMING_ENVIRONMENT = "pig.streaming.environment";

    /**
     * This key can be used to configure the python command for python streaming
     * udf. For eg, python2.7.
     */
    public static final String PIG_STREAMING_UDF_PYTHON_COMMAND = "pig.streaming.udf.python.command";

    // Pig input format settings
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

    // Pig output format settings
    /**
     * This key is used to define whether PigOutputFormat will be wrapped with LazyOutputFormat
     * so that jobs won't write empty part files if no output is generated
     */
    public static final String PIG_OUTPUT_LAZY = "pig.output.lazy";
    /**
     * This key is used to define whether to support recovery to handle the
     * application master getting restarted.
     */
    public static final String PIG_OUTPUT_COMMITTER_RECOVERY = "pig.output.committer.recovery.support";

    //Pig intermediate temporary file settings

    /**
     * Location where pig stores temporary files for job setup
     */
    public static final String PIG_TEMP_DIR = "pig.temp.dir";

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


    //Pig skewedjoin and order by sampling settings
    /**
     * For a given mean and a confidence, a sample rate is obtained from a poisson udf
     */
    public static final String PIG_POISSON_SAMPLER_SAMPLE_RATE = "pig.sksampler.samplerate";

    /**
     * This key used to control the sample size of RandomeSampleLoader for
     * order-by. The default value is 100 rows per task.
     */
    public static final String PIG_RANDOM_SAMPLER_SAMPLE_SIZE = "pig.random.sampler.sample.size";


    //Pig miscellaneous settings
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
     * This key is used to turn off the inclusion of settings in the jobs.
     */
    public static final String PIG_SCRIPT_INFO_ENABLED = "pig.script.info.enabled";

    /**
     * Controls the size of Pig script stored in job xml.
     */
    public static final String PIG_SCRIPT_MAX_SIZE = "pig.script.max.size";

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
     * Replication factor for files in pig jar cache
     */
    public static final String PIG_USER_CACHE_REPLICATION = "pig.user.cache.replication";

    /**
     * Boolean value used to enable or disable error handling for storers
     */
    public static final String PIG_ERROR_HANDLING_ENABLED = "pig.error-handling.enabled";

    /**
     * Controls the minimum number of errors
     */
    public static final String PIG_ERROR_HANDLING_MIN_ERROR_RECORDS = "pig.error-handling.min.error.records";

    /**
     * Set the threshold for percentage of errors
     */
    public static final String PIG_ERROR_HANDLING_THRESHOLD_PERCENT = "pig.error-handling.error.threshold";

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

    /**
     * The timezone to be used by Pig datetime datatype
     */
    public static final String PIG_DATETIME_DEFAULT_TIMEZONE = "pig.datetime.default.tz";

    /**
     * Using hadoop's TextInputFormat for reading bzip input instead of using Pig's Bzip2TextInputFormat. True by default
     * (only valid for 0.23/2.X)
     */
    public static final String PIG_BZIP_USE_HADOOP_INPUTFORMAT = "pig.bzip.use.hadoop.inputformat";

    /**
     * This key is used to set the download location when registering an artifact using ivy coordinate
     */
    public static final String PIG_ARTIFACTS_DOWNLOAD_LOCATION = "pig.artifacts.download.location";

    // Pig on Tez runtime settings
    /**
     * This key is used to define whether to reuse AM in Tez jobs.
     */
    public static final String PIG_TEZ_SESSION_REUSE = "pig.tez.session.reuse";

    /**
     * This key is used to configure the interval of dag status report in seconds. Default is 20
     */
    public static final String PIG_TEZ_DAG_STATUS_REPORT_INTERVAL = "pig.tez.dag.status.report.interval";


    // SpillableMemoryManager settings

    /**
     * Spill will be triggered if the fraction of biggest heap exceeds the usage threshold.
     * If {@link PigConfiguration.PIG_SPILL_UNUSED_MEMORY_THRESHOLD_SIZE} is non-zero, then usage threshold is calculated as
     * Max(HeapSize * PIG_SPILL_MEMORY_USAGE_THRESHOLD_FRACTION, HeapSize - PIG_SPILL_UNUSED_MEMORY_THRESHOLD_SIZE)
     * Default is 0.7
     */
    public static final String PIG_SPILL_MEMORY_USAGE_THRESHOLD_FRACTION = "pig.spill.memory.usage.threshold.fraction";

    /**
     * Spill will be triggered if the fraction of biggest heap exceeds the collection threshold.
     * If {@link PigConfiguration.PIG_SPILL_UNUSED_MEMORY_THRESHOLD_SIZE} is non-zero, then collection threshold is calculated as
     * Max(HeapSize * PIG_SPILL_COLLECTION_THRESHOLD_FRACTION, HeapSize - PIG_SPILL_UNUSED_MEMORY_THRESHOLD_SIZE)
     * Default is 0.7
     */
    public static final String PIG_SPILL_COLLECTION_THRESHOLD_FRACTION = "pig.spill.collection.threshold.fraction";

    /**
     * Spill will be triggered when unused memory falls below the threshold.
     * Default is 350MB
     */
    public static final String PIG_SPILL_UNUSED_MEMORY_THRESHOLD_SIZE = "pig.spill.unused.memory.threshold.size";

    /**
     * Log tracing id that can be used by upstream clients for tracking respective logs
     */
    public static final String PIG_LOG_TRACE_ID = "pig.log.trace.id";

    /**
     * Use Netty file server for Pig on Spark, true or false, default value is false
     */
    public static final String PIG_SPARK_USE_NETTY_FILESERVER = "pig.spark.rpc.useNettyFileServer";

    /**
     * Enable ATS for Pig
     */
    public static final String PIG_ATS_ENABLED = "pig.ats.enabled";

    /**
     * If set, Pig will override tez.am.launch.cmd-opts and tez.am.resource.memory.mb to optimal
     */
    public static final String PIG_TEZ_CONFIGURE_AM_MEMORY = "pig.tez.configure.am.memory";

    /**
     * If set to false, automatic schema disambiguation gets disabled i.e. group::name will be just name
     */
    public static final String PIG_STORE_SCHEMA_DISAMBIGUATE = "pig.store.schema.disambiguate";

    public static final String PIG_STORE_SCHEMA_DISAMBIGUATE_DEFAULT = "true";

    public static final String PIG_PRINT_EXEC_PLAN = "pig.print.exec.plan";

    /**
     * If GC is not explicitly set, default to parallelGC
     */
    public static final String PIG_GC_PARAMS = "pig.gc.default.params";

    public static final String PIG_GC_PARAMS_DEFAULT = "-XX:+UseParallelGC";

    public static final String PIG_GC_PATTERN = "-XX:\\+Use[a-zA-Z0-9_]+GC";

}
