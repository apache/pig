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

import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * This class provides an implementation of OrderedLoadFunc interface
 * which can be optionally re-used by LoadFuncs that use FileInputFormat, by
 * having this as a super class
 * @since Pig 0.7
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving // Since we haven't done outer join for merge join yet
public abstract class FileInputLoadFunc extends LoadFunc implements OrderedLoadFunc  {
    
    @Override
    public WritableComparable<?> getSplitComparable(InputSplit split)
    throws IOException{
        FileSplit fileSplit = null;
        if(split instanceof FileSplit){
            fileSplit = (FileSplit)split;
        }else{
            throw new RuntimeException("LoadFunc expected split of type FileSplit");
        }
        
        return new FileSplitComparable(
                fileSplit.getPath().toString(),
                fileSplit.getStart()
        );
    }

}
