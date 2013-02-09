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
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.piggybank.storage.HiveColumnarLoader;
import org.apache.pig.piggybank.storage.partition.PathPartitionHelper;

/**
 * HiveRCInputFormat used by HiveColumnarLoader as the InputFormat;
 * <p/>
 * Reasons for implementing a new InputFormat sub class:<br/>
 * <ul>
 * <li>The current RCFileInputFormat uses the old InputFormat mapred interface,
 * and the pig load store design used the new InputFormat mapreduce classes.</li>
 * <li>The splits are calculated by the InputFormat, HiveColumnarLoader supports
 * date partitions, the filtering is done here.</li>
 * </ul>
 */
public class HiveRCInputFormat extends
	FileInputFormat<LongWritable, BytesRefArrayWritable> {

    transient PathPartitionHelper partitionHelper = new PathPartitionHelper();

    String signature = "";

    public HiveRCInputFormat() {
	this(null);
    }

    public HiveRCInputFormat(String signature) {
	this.signature = signature;

	Properties properties = UDFContext.getUDFContext().getUDFProperties(
		HiveColumnarLoader.class, new String[] { signature });

	// This expression is passed in the
	// HiveColumnarLoader.setPartitionExpression method by the Pig Loader
	// Classes.
	String partitionExpression = properties
		.getProperty(PathPartitionHelper.PARITITION_FILTER_EXPRESSION);

	// backwards compatibility
	String dateRange = properties
		.getProperty(HiveColumnarLoader.DATE_RANGE);
	if (partitionExpression == null && dateRange != null) {
	    partitionExpression = buildFilterExpressionFromDatePartition(dateRange);
	    properties.setProperty(
		    PathPartitionHelper.PARITITION_FILTER_EXPRESSION,
		    partitionExpression);
	}

    }

    @Override
    protected List<FileStatus> listStatus(JobContext jobContext)
	    throws IOException {

	List<FileStatus> files = partitionHelper.listStatus(jobContext,
		HiveColumnarLoader.class, signature);

	if (files == null)
	    files = super.listStatus(jobContext);

	return files;

    }

    /**
     * If the date range was supplied in the loader constructor we need to build
     * our own filter expression.<br/>
     * 
     * @param dateRange
     * @return String
     */
    private String buildFilterExpressionFromDatePartition(String dateRange) {
	Properties properties = UDFContext.getUDFContext().getUDFProperties(
		HiveColumnarLoader.class, new String[] { signature });

	String partitionColumnStr = properties
		.getProperty(PathPartitionHelper.PARTITION_COLUMNS);

	boolean isYearMonthDayFormat = false;

	// only 3 partition types are supported (its impossible with date
	// partitions to support all possible combinations here).
	// 1) yyyy-MM-dd which is as /daydate=[date]/files
	// 2) yyyy-MM-dd which is as /date=[date]/files
	// 3) yyyy-MM-dd which is as /year=[year]/month=[month]/day=[day]
	String key = null;
	if (partitionColumnStr.contains("daydate")) {
	    key = "daydate"; // use daydate as key
	} else if (partitionColumnStr.contains("date")) {
	    key = "date"; // user date as key
	} else if (partitionColumnStr.contains("year")
		&& partitionColumnStr.contains("month")
		&& partitionColumnStr.contains("day")) {
	    isYearMonthDayFormat = true;
	} else {
	    throw new RuntimeException(
		    "Not date partitions where found for partitions: "
			    + partitionColumnStr);
	}

	String[] split = dateRange.split(":");

	if (split.length != 2) {
	    throw new RuntimeException(
		    "The date range must have format yyyy-MM-dd:yyyy-MM-dd");
	}

	String partitionExpression = null;
	if (isYearMonthDayFormat) {
	    // extract the YearMonthDay from the to dates;
	    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	    Date date1 = parseDate(dateFormat, split[0]);

	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date1);

	    partitionExpression = "(year >= '" + cal.get(Calendar.YEAR)
		    + "' and month >= '"
		    + formatNumber((cal.get(Calendar.MONTH) + 1))
		    + "' and day >= '"
		    + formatNumber(cal.get(Calendar.DAY_OF_MONTH)) + "')";

	    Date date2 = parseDate(dateFormat, split[1]);
	    cal.setTime(date2);

	    partitionExpression += " and (year <= '" + cal.get(Calendar.YEAR)
		    + "' and month <= '"
		    + formatNumber((cal.get(Calendar.MONTH) + 1))
		    + "' and day <= '"
		    + formatNumber(cal.get(Calendar.DAY_OF_MONTH)) + "')";

	} else {
	    partitionExpression = key + " >= '" + split[0] + "' and " + key
		    + " <= '" + split[1] + "'";
	}

	return partitionExpression;
    }

    private static final String formatNumber(int numb) {

	if (numb < 10) {
	    return "0" + numb;
	} else {
	    return "" + numb;
	}
    }

    /**
     * Initialises an instance of HiveRCRecordReader.
     */
    @Override
    public RecordReader<LongWritable, BytesRefArrayWritable> createRecordReader(
	    InputSplit split, TaskAttemptContext ctx) throws IOException,
	    InterruptedException {

	HiveRCRecordReader reader = new HiveRCRecordReader();

	return reader;
    }

    /**
     * Parse a date string with format yyyy-MM-dd.
     * 
     * @param dateFormat
     *            DateFormat
     * @param dateString
     *            String
     * @return Date
     */
    private static final Date parseDate(DateFormat dateFormat, String dateString) {
	try {
	    return dateFormat.parse(dateString);
	} catch (ParseException e) {
	    RuntimeException rt = new RuntimeException(e);
	    rt.setStackTrace(e.getStackTrace());
	    throw rt;
	}
    }

    /**
     * The input split size should never be smaller than the
     * RCFile.SYNC_INTERVAL
     */
    @Override
    protected long getFormatMinSplitSize() {
	return RCFile.SYNC_INTERVAL;
    }

}
