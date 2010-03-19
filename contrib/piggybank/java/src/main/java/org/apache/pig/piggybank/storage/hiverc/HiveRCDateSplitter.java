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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Expects the location to be a directory with the format dir/daydate=yyyy-MM-dd/{dirs or files}<br/>
 * Only dateDir(s) within the date range [date1, date2] will be returned in the method splitDirectory
 */
public class HiveRCDateSplitter {

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Start of date range
     */
    Date date1;
    /**
     * End of date range
     */
    Date date2;

    /**
     * 
     * @param dateRange String must have format yyyy-MM-dd:yyyy-MM-dd, the left most date is the start of the range.
     */
    public HiveRCDateSplitter(String dateRange){
        setupDateRange(dateRange);
    }


    /**
     * 
     * @param job
     * @param location
     * @return
     * @throws IOException
     */
    public List<FileStatus> splitDirectory(JobContext job, Path dir) throws IOException{

        FileSystem fs = dir.getFileSystem(job.getConfiguration());

        List<FileStatus> paths = new ArrayList<FileStatus>();

        if(fs.getFileStatus(dir).isDir()){
            //expect the structure dir/[datefolder]/{dirs or files}

            FileStatus[] dateDirs = fs.listStatus(dir);
            Path dateDirPath = null;

            for(FileStatus dateDirStatus : dateDirs){
                dateDirPath = dateDirStatus.getPath();

                //if the path is a directory and it is within the date range, add all of its sub-files
                if(dateDirStatus.isDir() && isInDateRange(dateDirPath.getName()))
                    addAllFiles(fs, dateDirPath, paths);


            }

        }

        return paths;
    }


    /**
     * Parse through the directory structure and for each file 
     * @param fs
     * @param dir
     * @param paths
     * @throws IOException
     */
    private final void addAllFiles(FileSystem fs, Path dir, List<FileStatus> paths) throws IOException{

        FileStatus[] files = fs.listStatus(dir);

        for(FileStatus fileStatus : files){

            if(fileStatus.isDir()){
                addAllFiles(fs, fileStatus.getPath(), paths);
            }else{
                paths.add(fileStatus);
            }

        }
    }

    /**
     * Extracts the daydate parameter from fileName and compares its date value with date1, and date2.
     * @param fileName
     * @return boolean true if the date value is between date1 and date2 inclusively
     */
    private final boolean isInDateRange(String fileName) {
        //if date ranges are to be applied, apply them and if the file daydate field 
        //is not in the date range set the shouldRead to false and return
        String currentDate;
        Date date;

        try {
            currentDate = HiveRCSchemaUtil.extractDayDate(fileName);
            date = dateFormat.parse(currentDate);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        int c1 = date.compareTo(date1);
        int c2 = date.compareTo(date2);


        return (c1 >= 0 && c2 <= 0);
    }

    /**
     * Parses the dateRange:String and creates the date1:Date and date2:Date instances forming the start and end of the date range.
     * @param dateRange
     */
    private void setupDateRange(String dateRange){
        //if a dateRange is specified apply date range filtering
        if(dateRange != null && dateRange.trim().length() > 0){
            String[] dates = dateRange.split(":");

            try {
                date1 = dateFormat.parse(dates[0]);
                date2 = dateFormat.parse(dates[1]);
            } catch (ParseException e) {
                throw new RuntimeException("The dateRange must have format yyyy-MM-dd:yyyy-MM-dd", e);
            }

        }
    }

}
