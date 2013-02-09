/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.pig.piggybank.storage.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * The InputFormat for avro data.
 *
 */
public class PigAvroInputFormat extends FileInputFormat<NullWritable, Writable> {

    private Schema schema = null;  /* avro schema */
    private boolean ignoreBadFiles = false; /* whether ignore corrupted files during load */

    /* if multiple avro record schemas are merged, this map associates each input
     * record with a remapping of its fields relative to the merged schema. please
     * see AvroStorageUtils.getSchemaToMergedSchemaMap() for more details.
     */
    private Map<Path, Map<Integer, Integer>> schemaToMergedSchemaMap;

    /**
     * empty constructor
     */
    public PigAvroInputFormat() {
    }

    /**
     * constructor called by AvroStorage to pass in schema and ignoreBadFiles.
     * @param schema input data schema
     * @param ignoreBadFiles whether ignore corrupted files during load
     * @param schemaToMergedSchemaMap map that associates each input record
     * with a remapping of its fields relative to the merged schema
     */
    public PigAvroInputFormat(Schema schema, boolean ignoreBadFiles,
            Map<Path, Map<Integer, Integer>> schemaToMergedSchemaMap) {
        this.schema = schema;
        this.ignoreBadFiles = ignoreBadFiles;
        this.schemaToMergedSchemaMap = schemaToMergedSchemaMap;
    }

    /**
     * Create and return an avro record reader.
     * It uses the input schema passed in to the
     * constructor.
     */
    @Override
    public RecordReader<NullWritable, Writable>
    createRecordReader(InputSplit split, TaskAttemptContext context)
    throws IOException,  InterruptedException {
        context.setStatus(split.toString());
        return new PigAvroRecordReader(context, (FileSplit) split, schema,
                ignoreBadFiles, schemaToMergedSchemaMap);
    }

}
