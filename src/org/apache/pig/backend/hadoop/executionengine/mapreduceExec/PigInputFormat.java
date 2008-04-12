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
package org.apache.pig.backend.hadoop.executionengine.mapreduceExec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pig.Slice;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.PigSlicer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.ValidatingInputFileSpec;
import org.apache.pig.impl.util.ObjectSerializer;

public class PigInputFormat implements InputFormat<Text, Tuple>,
        JobConfigurable {

    @SuppressWarnings("unchecked")
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        boolean isSplittable = job.getBoolean("pig.input.splittable", true);
        ArrayList<FileSpec> inputs = (ArrayList<FileSpec>) ObjectSerializer
                .deserialize(job.get("pig.inputs"));
        ArrayList<EvalSpec> mapFuncs = (ArrayList<EvalSpec>) ObjectSerializer
                .deserialize(job.get("pig.mapFuncs", ""));
        ArrayList<EvalSpec> groupFuncs = (ArrayList<EvalSpec>) ObjectSerializer
                .deserialize(job.get("pig.groupFuncs", ""));

        PigContext pigContext = (PigContext) ObjectSerializer.deserialize(job
                .get("pig.pigContext"));
        // TODO: don't understand this code
        // added for UNION: set group func arity to match arity of inputs
        if (groupFuncs != null && groupFuncs.size() != inputs.size()) {
            groupFuncs = new ArrayList<EvalSpec>();
            for (int i = 0; i < groupFuncs.size(); i++) {
                groupFuncs.set(i, null);
            }
        }

        if (inputs.size() != mapFuncs.size()) {
            StringBuilder sb = new StringBuilder();
            sb.append("number of inputs != number of map functions: ");
            sb.append(inputs.size());
            sb.append(" != ");
            sb.append(mapFuncs.size());
            sb.append(": ");
            sb.append(job.get("pig.mapFuncs", "missing"));
            throw new IOException(sb.toString());
        }
        if (groupFuncs!= null && inputs.size() != groupFuncs.size()) {
            StringBuilder sb = new StringBuilder();
            sb.append("number of inputs != number of group functions: ");
            sb.append(inputs.size());
            sb.append(" != ");
            sb.append(groupFuncs.size());
            throw new IOException(sb.toString());
        }

        FileSystem fs = FileSystem.get(job);
        List<SliceWrapper> splits = new ArrayList<SliceWrapper>();
        for (int i = 0; i < inputs.size(); i++) {
            DataStorage store = new HDataStorage(ConfigurationUtil.toProperties(job));
            ValidatingInputFileSpec spec;
            if (inputs.get(i) instanceof ValidatingInputFileSpec) {
                spec = (ValidatingInputFileSpec) inputs.get(i);
            } else {
                spec = new ValidatingInputFileSpec(inputs.get(i), store);
            }
            EvalSpec groupBy = groupFuncs == null ? null : groupFuncs.get(i);
            if (isSplittable && (spec.getSlicer() instanceof PigSlicer)) {
                ((PigSlicer)spec.getSlicer()).setSplittable(isSplittable);
            }
            Slice[] pigs = spec.getSlicer().slice(store, spec.getFileName());
            for (Slice split : pigs) {
                splits.add(new SliceWrapper(split, pigContext, groupBy,
                        mapFuncs.get(i), i, fs));
            }
        }
        return splits.toArray(new SliceWrapper[splits.size()]);
    }

    public RecordReader<Text, Tuple> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter) throws IOException {
        activeSplit = (SliceWrapper) split;
        return ((SliceWrapper) split).makeReader(job);
    }

    public void configure(JobConf conf) {
    }

    public static SliceWrapper getActiveSplit() {
        return activeSplit;
    }

    private static SliceWrapper activeSplit;

    public void validateInput(JobConf arg0) throws IOException {
    }

}
