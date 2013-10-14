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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.InternalMap;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.FindQuantiles;
import org.apache.pig.impl.io.NullableBigDecimalWritable;
import org.apache.pig.impl.io.NullableBigIntegerWritable;
import org.apache.pig.impl.io.NullableBooleanWritable;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDateTimeWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Utils;

public class WeightedRangePartitioner extends Partitioner<PigNullableWritable, Writable>
                                      implements Configurable {
    PigNullableWritable[] quantiles;
    RawComparator<PigNullableWritable> comparator;
    PigContext pigContext;
    final public static Map<PigNullableWritable,DiscreteProbabilitySampleGenerator> weightedParts
        = new HashMap<PigNullableWritable, DiscreteProbabilitySampleGenerator>();

    private static final Log log = LogFactory.getLog(WeightedRangePartitioner.class);

    Configuration job;

    @SuppressWarnings("unchecked")
    @Override
    public int getPartition(PigNullableWritable key, Writable value,
            int numPartitions){
        if (comparator == null) {
            comparator = (RawComparator<PigNullableWritable>)PigMapReduce.sJobContext.getSortComparator();
        }

        if(!weightedParts.containsKey(key)){
            int index = Arrays.binarySearch(quantiles, key, comparator);
            if (index < 0)
                index = -index-1;
            else
                index = index + 1;
            return Math.min(index, numPartitions - 1);
        }
        DiscreteProbabilitySampleGenerator gen = weightedParts.get(key);
        return gen.getNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void setConf(Configuration configuration) {
        job = configuration;

        try {
            pigContext = (PigContext)ObjectSerializer.deserialize(job.get("pig.pigContext"));
        } catch (IOException e1) {
            // should not happen
            e1.printStackTrace();
        }

        String quantilesFile = configuration.get("pig.quantilesFile", "");

        if (quantilesFile.length() == 0) {
            throw new RuntimeException(this.getClass().getSimpleName()
                    + " used but no quantiles found");
        }

        try{
            // use local file system to get the quantilesFile
            Configuration conf;
            if (!pigContext.getExecType().isLocal()) {
                conf = new Configuration(true);
            } else {
                conf = new Configuration(false);
            }
            if (configuration.get("fs.file.impl") != null) {
                conf.set("fs.file.impl", configuration.get("fs.file.impl"));
            }
            if (configuration.get("fs.hdfs.impl") != null) {
                conf.set("fs.hdfs.impl", configuration.get("fs.hdfs.impl"));
            }

            MapRedUtil.copyTmpFileConfigurationValues(configuration, conf);

            conf.set(MapRedUtil.FILE_SYSTEM_NAME, "file:///");

            ReadToEndLoader loader = new ReadToEndLoader(Utils.getTmpFileStorageObject(conf),
                    conf, quantilesFile, 0);
            DataBag quantilesList;
            Tuple t = loader.getNext();
            if (t != null) {
                // the Quantiles file has a tuple as under:
                // (numQuantiles, bag of samples)
                // numQuantiles here is the reduce parallelism
                Map<String, Object> quantileMap = (Map<String, Object>) t.get(0);
                quantilesList = (DataBag) quantileMap.get(FindQuantiles.QUANTILES_LIST);
                InternalMap weightedPartsData = (InternalMap) quantileMap.get(FindQuantiles.WEIGHTED_PARTS);
                convertToArray(quantilesList);
                for(Entry<Object, Object> ent : weightedPartsData.entrySet()){
                    Tuple key = (Tuple)ent.getKey(); // sample item which repeats
                    float[] probVec = getProbVec((Tuple)ent.getValue());
                    weightedParts.put(getPigNullableWritable(key),
                            new DiscreteProbabilitySampleGenerator(probVec));
                }
            }
            // else - the quantiles file is empty - unless we have a bug, the
            // input must also be empty in which case we don't need to put
            // anything in weightedParts since getPartition() should never get
            // called. If the quantiles file is empty due to either a bug or
            // a transient failure situation on the dfs, then weightedParts will
            // not be populated and the job will fail in getPartition()
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * @param value
     * @return
     * @throws ExecException
     */
    private float[] getProbVec(Tuple values) throws ExecException {
        float[] probVec = new float[values.size()];
        for(int i = 0; i < values.size(); i++) {
            probVec[i] = (Float)values.get(i);
        }
        return probVec;
    }

    private PigNullableWritable getPigNullableWritable(Tuple t) {
        try {
            // user comparators work with tuples - so if user comparator
            // is being used OR if there are more than 1 sort cols, use
            // NullableTuple
            if ("true".equals(job.get("pig.usercomparator")) || t.size() > 1) {
                return new NullableTuple(t);
            } else {
                Object o = t.get(0);
                String kts = job.get("pig.reduce.key.type");
                if (kts == null) {
                    throw new RuntimeException("Didn't get reduce key type "
                        + "from config file.");
                }
                return HDataType.getWritableComparableTypes(o,
                    Byte.valueOf(kts));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void convertToArray(
            DataBag quantilesListAsBag) {
        ArrayList<PigNullableWritable> quantilesList = getList(quantilesListAsBag);
        if ("true".equals(job.get("pig.usercomparator")) ||
                quantilesList.get(0).getClass().equals(NullableTuple.class)) {
            quantiles = quantilesList.toArray(new NullableTuple[0]);
        } else if (quantilesList.get(0).getClass().equals(NullableBytesWritable.class)) {
            quantiles = quantilesList.toArray(new NullableBytesWritable[0]);
        } else if (quantilesList.get(0).getClass().equals(NullableDoubleWritable.class)) {
            quantiles = quantilesList.toArray(new NullableDoubleWritable[0]);
        } else if (quantilesList.get(0).getClass().equals(NullableBigIntegerWritable.class)) {
            quantiles = quantilesList.toArray(new NullableBigIntegerWritable[0]);
        } else if (quantilesList.get(0).getClass().equals(NullableBigDecimalWritable.class)) {
            quantiles = quantilesList.toArray(new NullableBigDecimalWritable[0]);
        } else if (quantilesList.get(0).getClass().equals(NullableFloatWritable.class)) {
            quantiles = quantilesList.toArray(new NullableFloatWritable[0]);
        } else if (quantilesList.get(0).getClass().equals(NullableBooleanWritable.class)) {
            quantiles = quantilesList.toArray(new NullableBooleanWritable[0]);
        } else if (quantilesList.get(0).getClass().equals(NullableIntWritable.class)) {
            quantiles = quantilesList.toArray(new NullableIntWritable[0]);
        } else if (quantilesList.get(0).getClass().equals(NullableLongWritable.class)) {
            quantiles = quantilesList.toArray(new NullableLongWritable[0]);
        } else if (quantilesList.get(0).getClass().equals(NullableDateTimeWritable.class)) {
            quantiles = quantilesList.toArray(new NullableDateTimeWritable[0]);
        } else if (quantilesList.get(0).getClass().equals(NullableText.class)) {
            quantiles = quantilesList.toArray(new NullableText[0]);
        } else {
            throw new RuntimeException("Unexpected class in " + this.getClass().getSimpleName());
        }
    }

    /**
     * @param quantilesListAsBag
     * @return
     */
    private ArrayList<PigNullableWritable> getList(DataBag quantilesListAsBag) {

        ArrayList<PigNullableWritable> list = new ArrayList<PigNullableWritable>();
        for (Tuple tuple : quantilesListAsBag) {
            list.add(getPigNullableWritable(tuple));
        }
        return list;
    }

    @Override
    public Configuration getConf() {
        return job;
    }


}
