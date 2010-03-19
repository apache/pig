/**
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
package org.apache.pig.piggybank.storage.hiverc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * HiveRCInputFormat used by HiveColumnarLoader as the InputFormat;
 * <p/>
 * Reasons for implementing a new InputFormat sub class:<br/>
 * <ul>
 *  <li>The current RCFileInputFormat uses the old InputFormat mapred interface, and the pig load store design used the new InputFormat mapreduce classes.</li>
 *  <li>The splits are calculated by the InputFormat, HiveColumnarLoader supports date partitions, the filtering is done here.</li>
 * </ul>
 */
public class HiveRCInputFormat extends FileInputFormat<LongWritable, BytesRefArrayWritable>{

    /**
     * Implements the date splitting logic, this keeps the HiveRCInputFormat clean of the date calculations,
     * and makes extending it to support other types of partitioning in the future easier. 
     */
    private HiveRCDateSplitter dateSplitter;
    /**
     * Only true if the HiveRCInputFormat(dateRange:String) constructor is used
     */
    private boolean applyDateRanges = false;

    /**
     * No date partitioning is applied
     */
    public HiveRCInputFormat(){
    }

    /**
     * Date partitioning will be applied to the input path.<br/>
     * The path must be partitioned as input-path/daydate=yyyy-MM-dd.
     * @param dateRange Must have format yyyy-MM-dd:yyyy-MM-dd with the left most being the start of the range.
     */
    public HiveRCInputFormat(String dateRange){
        applyDateRanges = true;
        dateSplitter = new HiveRCDateSplitter(dateRange);
    }

    /**
     * Initialises an instance of HiveRCRecordReader.
     */
    @Override
    public RecordReader<LongWritable, BytesRefArrayWritable> createRecordReader(
            InputSplit split, TaskAttemptContext ctx) throws IOException,
            InterruptedException {

        HiveRCRecordReader reader = new HiveRCRecordReader();
        reader.initialize(split, ctx);

        return reader;
    }

    /**
     * This method is called by the FileInputFormat to find the input paths for which splits should be calculated.<br/>
     * If applyDateRanges == true: Then the HiveRCDateSplitter is used to apply filtering on the input files.<br/>
     * Else the default FileInputFormat listStatus method is used. 
     */
    @Override
    protected List<FileStatus> listStatus(JobContext ctx)throws IOException {
        //for each path in the FileInputFormat input paths, create a split.
        //If applyDateRanges:
        //the date logic is handled in the HiveRCLoader where the FileInputFormat inputPaths is set
        //to include only the files within the given date range, when date range applies
        //Else
        // add all files
        Path[] inputPaths = FileInputFormat.getInputPaths(ctx);

        List<FileStatus> splitPaths = new ArrayList<FileStatus>();

        if(applyDateRanges){
            //use the dateSplitter to calculate only those paths that are in the correct date partition 
            for(Path inputPath : inputPaths){
                splitPaths.addAll(dateSplitter.splitDirectory(ctx, inputPath));
            }
        }else{
            //use the default implementation
            splitPaths = super.listStatus(ctx);
        }

        return splitPaths;
    }

    /**
     * The input split size should never be smaller than the RCFile.SYNC_INTERVAL
     */
    @Override
    protected long getFormatMinSplitSize(){	      
        return RCFile.SYNC_INTERVAL;
    }

}
