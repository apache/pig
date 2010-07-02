/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * RegExLoader is an abstract class used to parse logs based on a regular expression.
 * 
 * There is a single abstract method, getPattern which needs to return a Pattern. Each group will be returned
 * as a different DataAtom.
 * 
 * Look to org.apache.pig.piggybank.storage.apachelog.CommonLogLoader for example usage.
 */

public abstract class RegExLoader extends LoadFunc {
  private LineRecordReader in = null;
  
  abstract public Pattern getPattern();

  @Override
  public Tuple getNext() throws IOException {
    Pattern pattern = getPattern();
    Matcher matcher = pattern.matcher("");
    TupleFactory mTupleFactory = DefaultTupleFactory.getInstance();
    String line;
    
    while (in.nextKeyValue()) {
	  Text val = in.getCurrentValue();
      line = val.toString();
      if (line.length() > 0 && line.charAt(line.length() - 1) == '\r') {
        line = line.substring(0, line.length() - 1);
      }
      matcher = matcher.reset(line);
      ArrayList<DataByteArray> list = new ArrayList<DataByteArray>();
      if (matcher.find()) {
        for (int i = 1; i <= matcher.groupCount(); i++) {
          list.add(new DataByteArray(matcher.group(i)));
        }
        return mTupleFactory.newTuple(list);  
      }
    }
    return null;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public InputFormat getInputFormat() throws IOException {
      return new TextInputFormat();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToRead(RecordReader reader, PigSplit split)
          throws IOException {
      in = (LineRecordReader) reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
      FileInputFormat.setInputPaths(job, location);      
  }

}
