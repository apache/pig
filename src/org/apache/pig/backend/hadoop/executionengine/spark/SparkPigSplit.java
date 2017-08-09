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

package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;

import com.google.common.collect.Lists;

/**
 * Wrapper class for PigSplits in Spark mode
 *
 * Spark only counts HDFS bytes read if we provide a FileSplit, so we have to wrap PigSplits and have the wrapper
 * class extend FileSplit
 */
public interface SparkPigSplit extends Writable, Configurable, Serializable {

   InputSplit getWrappedSplit();

   InputSplit getWrappedSplit(int idx);

   SplitLocationInfo[] getLocationInfo() throws IOException;

   long getLength(int idx) throws IOException, InterruptedException;

   int getSplitIndex();

   void setMultiInputs(boolean b);

   boolean isMultiInputs();

   int getNumPaths();

   void setDisableCounter(boolean disableCounter);

   boolean disableCounter();

   void setCurrentIdx(int idx);

   PigSplit getWrappedPigSplit();

   public static class FileSparkPigSplit extends FileSplit implements SparkPigSplit {

       private PigSplit pigSplit;

       /**
        * Spark executor's deserializer calls this, and we have to instantiate a default wrapped object
        */
       public FileSparkPigSplit () {
           pigSplit = new PigSplit();
       }

       public FileSparkPigSplit(PigSplit pigSplit) {
           this.pigSplit = pigSplit;
       }

       @Override
       public SplitLocationInfo[] getLocationInfo() throws IOException {
           return pigSplit.getLocationInfo();
       }

       @Override
       public String toString() {
           return pigSplit.toString();
       }

       @Override
       public long getLength() {
           try {
               return pigSplit.getLength();
           } catch (IOException | InterruptedException e) {
               throw new RuntimeException(e);
           }
       }

       @Override
       public String[] getLocations() throws IOException {
           try {
               return pigSplit.getLocations();
           } catch (InterruptedException e) {
               throw new RuntimeException(e);
           }
       }

       @Override
       public InputSplit getWrappedSplit() {
           return pigSplit.getWrappedSplit();
       }

       @Override
       public InputSplit getWrappedSplit(int idx) {
           return pigSplit.getWrappedSplit(idx);
       }

       @Override
       public long getLength(int idx) throws IOException, InterruptedException {
           return pigSplit.getLength(idx);
       }

       @Override
       public void readFields(DataInput is) throws IOException {
           this.getConf().readFields(is);
           pigSplit.readFields(is);
       }

       @Override
       public void write(DataOutput os) throws IOException {
           SparkPigSplitsUtils.writeConfigForPigSplits(this.getConf(), os);
           pigSplit.write(os);
       }

       @Override
       public int getSplitIndex() {
           return pigSplit.getSplitIndex();
       }

       @Override
       public void setMultiInputs(boolean b) {
           pigSplit.setMultiInputs(b);
       }

       @Override
       public boolean isMultiInputs() {
           return pigSplit.isMultiInputs();
       }

       @Override
       public Configuration getConf() {
           return pigSplit.getConf();
       }

       @Override
       public void setConf(Configuration conf) {
           pigSplit.setConf(conf);
       }

       @Override
       public int getNumPaths() {
           return pigSplit.getNumPaths();
       }

       @Override
       public void setDisableCounter(boolean disableCounter) {
           pigSplit.setDisableCounter(disableCounter);
       }

       @Override
       public boolean disableCounter() {
           return pigSplit.disableCounter();
       }

       @Override
       public void setCurrentIdx(int idx) {
           pigSplit.setCurrentIdx(idx);
       }

       @Override
       public PigSplit getWrappedPigSplit() {
           return this.pigSplit;
       }

       @Override
       public Path getPath() {
           return ((FileSplit)getWrappedPigSplit().getWrappedSplit()).getPath();
       }
   }

    public static class GenericSparkPigSplit extends InputSplit implements SparkPigSplit {

        private static final long serialVersionUID = 1L;

        private PigSplit pigSplit;

        /**
         * Spark executor's deserializer calls this, and we have to instantiate a default wrapped object
         */
        public GenericSparkPigSplit() {
            pigSplit = new PigSplit();
        }

        public GenericSparkPigSplit(PigSplit pigSplit) {
            this.pigSplit = pigSplit;
        }

        @Override
        public SplitLocationInfo[] getLocationInfo() throws IOException {
            return pigSplit.getLocationInfo();
        }

        @Override
        public String toString() {
            return pigSplit.toString();
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return pigSplit.getLength();
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return pigSplit.getLocations();
        }

        @Override
        public InputSplit getWrappedSplit() {
            return pigSplit.getWrappedSplit();
        }

        @Override
        public InputSplit getWrappedSplit(int idx) {
            return pigSplit.getWrappedSplit(idx);
        }

        @Override
        public long getLength(int idx) throws IOException, InterruptedException {
            return pigSplit.getLength(idx);
        }

        @Override
        public void readFields(DataInput is) throws IOException {
            this.getConf().readFields(is);
            pigSplit.readFields(is);
        }

        @Override
        public void write(DataOutput os) throws IOException {
            SparkPigSplitsUtils.writeConfigForPigSplits(this.getConf(), os);
            pigSplit.write(os);
        }

        @Override
        public int getSplitIndex() {
            return pigSplit.getSplitIndex();
        }

        @Override
        public void setMultiInputs(boolean b) {
            pigSplit.setMultiInputs(b);
        }

        @Override
        public boolean isMultiInputs() {
            return pigSplit.isMultiInputs();
        }

        @Override
        public Configuration getConf() {
            return pigSplit.getConf();
        }

        @Override
        public void setConf(Configuration conf) {
            pigSplit.setConf(conf);
        }

        @Override
        public int getNumPaths() {
            return pigSplit.getNumPaths();
        }

        @Override
        public void setDisableCounter(boolean disableCounter) {
            pigSplit.setDisableCounter(disableCounter);
        }

        @Override
        public boolean disableCounter() {
            return pigSplit.disableCounter();
        }

        @Override
        public void setCurrentIdx(int idx) {
            pigSplit.setCurrentIdx(idx);
        }

        @Override
        public PigSplit getWrappedPigSplit() {
            return this.pigSplit;
        }
    }

    public static class SparkPigSplitsUtils {

        private static final List<String> PIGSPLIT_CONFIG_KEYS = Lists.newArrayList(
                CommonConfigurationKeys.IO_SERIALIZATIONS_KEY,
                PigConfiguration.PIG_COMPRESS_INPUT_SPLITS
        );

        /**
         * Writes a subset of the originalConf into the output stream os. Only keys in PIG_SPLIT_CONFIG_KEYS are
         * considered due to optimization purposes. During deseralization on a Spark executor we need to take care of
         * setting the configuration manually because Spark only sets an empty Configuration instance on the PigSplit.
         * @param originalConf
         * @param os
         * @throws IOException
         */
        public static void writeConfigForPigSplits(Configuration originalConf, DataOutput os) throws IOException {
            Configuration conf = new Configuration(false);
            for (String key : PIGSPLIT_CONFIG_KEYS) {
                String value = originalConf.get(key);
                if (value != null) {
                    conf.set(key, value);
                }
            }
            conf.write(os);
        }

    }

}
