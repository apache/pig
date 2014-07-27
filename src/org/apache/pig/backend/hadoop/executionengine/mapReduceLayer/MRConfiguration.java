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

public class MRConfiguration {

    public static final String CHILD_JAVA_OPTS = "mapred.child.java.opts";
    public static final String FILEOUTPUTCOMMITTER_MARKSUCCESSFULJOBS = "mapreduce.fileoutputcommitter.marksuccessfuljobs";
    public static final String FRAMEWORK_NAME = "mapreduce.framework.name";
    public static final String INPUT_DIR = "mapred.input.dir";
    public static final String INPUT_DIR_RECURSIVE = "mapred.input.dir.recursive";
    public static final String INPUTFORMAT_CLASS = "mapreduce.inputformat.class";
    public static final String IO_SORT_MB = "mapred.io.sort.mb";
    public static final String JAR = "mapred.jar";
    public static final String JOB_APPLICATION_ATTEMPT_ID = "mapreduce.job.application.attempt.id";
    public static final String JOB_CACHE_FILES = "mapreduce.job.cache.files";
    public static final String JOB_CREDENTIALS_BINARY = "mapreduce.job.credentials.binary";
    public static final String JOB_CREDENTIALS_JSON = "mapreduce.job.credentials.json";
    public static final String JOB_END_NOTIFICATION_RETRY_INTERVAL = "mapreduce.job.end-notification.retry.interval";
    public static final String JOB_HDFS_SERVERS = "mapreduce.job.hdfs-servers";
    public static final String JOB_ID = "mapred.job.id";
    public static final String JOB_NAME = "mapred.job.name";
    public static final String JOB_PRIORITY = "mapred.job.priority";
    public static final String JOB_QUEUE_NAME = "mapred.job.queue.name";
    public static final String JOB_REDUCE_MARKRESET_BUFFER_PERCENT = "mapred.job.reduce.markreset.buffer.percent";
    public static final String JOB_TRACKER = "mapred.job.tracker";
    public static final String JOB_TRACKER_HTTP_ADDRESS = "mapred.job.tracker.http.address";
    public static final String JOB_REDUCES = "mapreduce.job.reduces";
    public static final String LINERECORDREADER_MAXLENGTH = "mapred.linerecordreader.maxlength";
    public static final String MAP_MAX_ATTEMPTS = "mapred.map.max.attempts";
    public static final String MAP_TASKS = "mapred.map.tasks";
    public static final String MAPPER_NEW_API = "mapred.mapper.new-api";
    public static final String MAX_SPLIT_SIZE = "mapred.max.split.size";
    public static final String OUTPUT_BASENAME = "mapreduce.output.basename";
    public static final String OUTPUT_COMPRESS = "mapred.output.compress";
    public static final String OUTPUT_COMPRESSION_CODEC = "mapred.output.compression.codec";
    public static final String OUTPUT_DIR = "mapred.output.dir";
    public static final String REDUCE_MAX_ATTEMPTS = "mapred.reduce.max.attempts";
    public static final String REDUCE_TASKS = "mapred.reduce.tasks";
    public static final String REDUCER_NEW_API = "mapred.reducer.new-api";
    public static final String SUMIT_REPLICATION = "mapred.submit.replication";
    public static final String TASK_ID = "mapred.task.id";
    public static final String TASK_IS_MAP = "mapred.task.is.map";
    public static final String TASK_PARTITION = "mapred.task.partition";
    public static final String TASK_TIMEOUT = "mapred.task.timeout";
    public static final String TEXTOUTPUTFORMAT_SEPARATOR = "mapred.textoutputformat.separator";
    public static final String WORK_OUPUT_DIR = "mapred.work.output.dir";

}
