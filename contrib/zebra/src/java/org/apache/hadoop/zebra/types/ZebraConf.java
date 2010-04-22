/**
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

package org.apache.hadoop.zebra.types;

import org.apache.hadoop.conf.Configuration;

/**
 * Zebra's implementation of Pig's Tuple.
 * It's derived from Pig's DefaultTuple implementation.
 * 
 */
public class ZebraConf {
  // input configurations
  
  // output configurations
  private static final String MAPREDUCE_OUTPUT_PATH = "mapreduce.lib.table.output.dir";
  private static final String MAPREDUCE_MULTI_OUTPUT_PATH = "mapreduce.lib.table.multi.output.dirs";
  private static final String MAPREDUCE_OUTPUT_SCHEMA = "mapreduce.lib.table.output.schema";
  private static final String MAPREDUCE_OUTPUT_STORAGEHINT = "mapreduce.lib.table.output.storagehint";
  private static final String MAPREDUCE_OUTPUT_SORTCOLUMNS = "mapreduce.lib.table.output.sortcolumns";
  private static final String MAPREDUCE_OUTPUT_COMPARATOR =  "mapreduce.lib.table.output.comparator";
  private static final String IS_MULTI = "multi";
  private static final String ZEBRA_OUTPUT_PARTITIONER_CLASS = "zebra.output.partitioner.class";
  private static final String ZEBRA_OUTPUT_PARTITIONER_CLASS_ARGUMENTS = "zebra.output.partitioner.class.arguments";
  private static final String MAPREDUCE_OUTPUT_CHECKTYPE = "mapreduce.lib.table.output.checktype";
  
  private static final String MAPRED_OUTPUT_PATH = "mapred.lib.table.output.dir";
  private static final String MAPRED_MULTI_OUTPUT_PATH = "mapred.lib.table.multi.output.dirs";
  private static final String MAPRED_OUTPUT_SCHEMA = "mapred.lib.table.output.schema";
  private static final String MAPRED_OUTPUT_STORAGEHINT = "mapred.lib.table.output.storagehint";
  private static final String MAPRED_OUTPUT_SORTCOLUMNS = "mapred.lib.table.output.sortcolumns";
  private static final String MAPRED_OUTPUT_COMPARATOR =  "mapred.lib.table.output.comparator";
  

    
  static public String getOutputPath(Configuration conf) {
    return conf.get(MAPREDUCE_OUTPUT_PATH) != null? conf.get(MAPREDUCE_OUTPUT_PATH): conf.get(MAPRED_OUTPUT_PATH);
  }
  
  static public void setOutputPath(Configuration conf, String value) {
    conf.set(MAPREDUCE_OUTPUT_PATH, value);
  }
  
  static public String getMultiOutputPath(Configuration conf) {
    return conf.get(MAPREDUCE_MULTI_OUTPUT_PATH) != null? conf.get(MAPREDUCE_MULTI_OUTPUT_PATH) : conf.get(MAPRED_MULTI_OUTPUT_PATH);
  }
  
  static public void setMultiOutputPath(Configuration conf, String value) {
    conf.set(MAPREDUCE_MULTI_OUTPUT_PATH, value);
  }
  
  static public String getOutputSchema(Configuration conf) {
    return conf.get(MAPREDUCE_OUTPUT_SCHEMA) != null? conf.get(MAPREDUCE_OUTPUT_SCHEMA): conf.get(MAPRED_OUTPUT_SCHEMA); 
  }
  
  static public void setOutputSchema(Configuration conf, String value) {
    conf.set(MAPREDUCE_OUTPUT_SCHEMA, value);
  }
  
  static public String getOutputStorageHint(Configuration conf) {
    return conf.get(MAPREDUCE_OUTPUT_STORAGEHINT) != null? conf.get(MAPREDUCE_OUTPUT_STORAGEHINT) : 
      conf.get(MAPRED_OUTPUT_STORAGEHINT) != null? conf.get(MAPRED_OUTPUT_STORAGEHINT) : "";   
  }
  
  static public void setOutputStorageHint(Configuration conf, String value) {
    conf.set(MAPREDUCE_OUTPUT_STORAGEHINT, value);
  }
  
  static public String getOutputSortColumns(Configuration conf) {
    return conf.get(MAPREDUCE_OUTPUT_SORTCOLUMNS) != null? conf.get(MAPREDUCE_OUTPUT_SORTCOLUMNS) : conf.get(MAPRED_OUTPUT_SORTCOLUMNS);
  }
  
  static public void setOutputSortColumns(Configuration conf, String value) {
    if (value != null) {
      conf.set(MAPREDUCE_OUTPUT_SORTCOLUMNS, value);
    }
  }
  
  static public String getOutputComparator(Configuration conf) {
    return conf.get(MAPREDUCE_OUTPUT_COMPARATOR) != null? conf.get(MAPREDUCE_OUTPUT_COMPARATOR): conf.get(MAPRED_OUTPUT_COMPARATOR);
  }
  
  static public void setOutputComparator(Configuration conf, String value) {
    conf.set(MAPREDUCE_OUTPUT_COMPARATOR, value);
  }
  
  static public Boolean getIsMulti(Configuration conf, boolean defaultValue) {
    return conf.getBoolean(IS_MULTI, defaultValue);
  }
  
  static public void setIsMulti(Configuration conf, boolean value) {
    conf.setBoolean(IS_MULTI, value);
  }
  
  static public Boolean getCheckType(Configuration conf, boolean defaultValue) {
    return conf.getBoolean(MAPREDUCE_OUTPUT_CHECKTYPE, defaultValue);
  }
  
  static public void setCheckType(Configuration conf, boolean value) {
    conf.setBoolean(MAPREDUCE_OUTPUT_CHECKTYPE, value);
  }
  
  static public String getZebraOutputPartitionerClass(Configuration conf) {
    return conf.get(ZEBRA_OUTPUT_PARTITIONER_CLASS);
  }
  
  static public void setZebraOutputPartitionerClass(Configuration conf, String value) {
    conf.set(ZEBRA_OUTPUT_PARTITIONER_CLASS, value);
  }
  
  static public String getOutputPartitionClassArguments(Configuration conf) {
    return conf.get(ZEBRA_OUTPUT_PARTITIONER_CLASS_ARGUMENTS);
  }
  
  static public void setOutputPartitionClassArguments(Configuration conf, String value) {
    conf.set(ZEBRA_OUTPUT_PARTITIONER_CLASS_ARGUMENTS, value);
  }  
}