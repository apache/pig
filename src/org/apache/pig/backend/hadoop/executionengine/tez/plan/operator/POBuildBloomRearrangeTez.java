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
package org.apache.pig.backend.hadoop.executionengine.tez.plan.operator;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;

/**
 * This operator writes out the key value for the hash join reduce operation similar to POLocalRearrangeTez.
 * In addition, it also writes out the bloom filter constructed from the join keys
 * in the case of bloomjoin map strategy or join keys themselves in case of reduce strategy.
 *
 * Using multiple bloom filters partitioned by the hash of the key allows for parallelism.
 * It also allows us to have lower false positives with smaller vector sizes.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class POBuildBloomRearrangeTez extends POLocalRearrangeTez {
    private static final long serialVersionUID = 1L;
    private static final Log LOG = LogFactory.getLog(POBuildBloomRearrangeTez.class);

    public static final String DEFAULT_BLOOM_STRATEGY = "map";
    public static final int DEFAULT_NUM_BLOOM_FILTERS_REDUCE = 11;
    public static final int DEFAULT_NUM_BLOOM_HASH_FUNCTIONS = 3;
    public static final String DEFAULT_BLOOM_HASH_TYPE = "murmur";
    public static final int DEFAULT_BLOOM_VECTOR_SIZE_BYTES = 1024 * 1024;

    private String bloomOutputKey;
    private boolean skipNullKeys = false;
    private boolean createBloomInMap;
    private int numBloomFilters;
    private int vectorSizeBytes;
    private int numHash;
    private int hashType;

    private transient BloomFilter[] bloomFilters;
    private transient KeyValueWriter bloomWriter;
    private transient PigNullableWritable nullKey;
    private transient Tuple bloomValue;
    private transient NullableTuple bloomNullableTuple;

    public POBuildBloomRearrangeTez(POLocalRearrangeTez lr,
            boolean createBloomInMap, int numBloomFilters, int vectorSizeBytes,
            int numHash, int hashType) {
        super(lr);
        this.createBloomInMap = createBloomInMap;
        this.numBloomFilters = numBloomFilters;
        this.vectorSizeBytes = vectorSizeBytes;
        this.numHash = numHash;
        this.hashType = hashType;
    }

    public static int getNumBloomFilters(Configuration conf) {
        if ("map".equals(conf.get(PigConfiguration.PIG_BLOOMJOIN_STRATEGY, DEFAULT_BLOOM_STRATEGY))) {
            return conf.getInt(PigConfiguration.PIG_BLOOMJOIN_NUM_FILTERS, 1);
        } else {
            return conf.getInt(PigConfiguration.PIG_BLOOMJOIN_NUM_FILTERS, DEFAULT_NUM_BLOOM_FILTERS_REDUCE);
        }
    }

    public void setSkipNullKeys(boolean skipNullKeys) {
        this.skipNullKeys = skipNullKeys;
    }

    public void setBloomOutputKey(String bloomOutputKey) {
        this.bloomOutputKey = bloomOutputKey;
    }

    @Override
    public boolean containsOutputKey(String key) {
        if(super.containsOutputKey(key)) {
            return true;
        }
        return bloomOutputKey.equals(key);
    }

    @Override
    public String[] getTezOutputs() {
        return new String[] { outputKey, bloomOutputKey };
    }

    @Override
    public void replaceOutput(String oldOutputKey, String newOutputKey) {
        if (oldOutputKey.equals(outputKey)) {
            outputKey = newOutputKey;
        } else if (oldOutputKey.equals(bloomOutputKey)) {
            bloomOutputKey = newOutputKey;
        }
    }

    @Override
    public void attachOutputs(Map<String, LogicalOutput> outputs,
            Configuration conf) throws ExecException {
        super.attachOutputs(outputs, conf);
        LogicalOutput output = outputs.get(bloomOutputKey);
        if (output == null) {
            throw new ExecException("Output to vertex " + bloomOutputKey + " is missing");
        }
        try {
            bloomWriter = (KeyValueWriter) output.getWriter();
            LOG.info("Attached output to vertex " + bloomOutputKey + " : output=" + output + ", writer=" + bloomWriter);
        } catch (Exception e) {
            throw new ExecException(e);
        }
        bloomFilters = new BloomFilter[numBloomFilters];
        bloomValue = mTupleFactory.newTuple(1);
        bloomNullableTuple = new NullableTuple(bloomValue);
    }

    @Override
    public Result getNextTuple() throws ExecException {

        PigNullableWritable key;
        while (true) {
            res = super.getRearrangedTuple();
            try {
                switch (res.returnStatus) {
                case POStatus.STATUS_OK:
                    if (illustrator == null) {
                        Tuple result = (Tuple) res.result;
                        Byte index = (Byte) result.get(0);

                        Object keyObj = result.get(1);
                        if (keyObj != null) {
                            key = HDataType.getWritableComparableTypes(keyObj, keyType);
                            // null keys cannot be part of bloom filter
                            // Since they are also dropped during join we can skip them
                            if (createBloomInMap) {
                                addKeyToBloomFilter(keyObj);
                            } else {
                                writeJoinKeyForBloom(keyObj);
                            }
                        } else if (skipNullKeys) {
                            // Inner join. So don't bother writing null key
                            continue;
                        } else {
                            if (nullKey == null) {
                                nullKey = HDataType.getWritableComparableTypes(keyObj, keyType);
                            }
                            key = nullKey;
                        }

                        NullableTuple val = new NullableTuple((Tuple)result.get(2));
                        key.setIndex(index);
                        val.setIndex(index);
                        writer.write(key, val);
                    } else {
                        illustratorMarkup(res.result, res.result, 0);
                    }
                    continue;
                case POStatus.STATUS_NULL:
                    continue;
                case POStatus.STATUS_EOP:
                    if (this.parentPlan.endOfAllInput && createBloomInMap) {
                        // In case of Split will get EOP after every record.
                        // So check for endOfAllInput
                        writeBloomFilters();
                    }
                case POStatus.STATUS_ERR:
                default:
                    return res;
                }
            } catch (IOException ioe) {
                int errCode = 2135;
                String msg = "Received error from POBuildBloomRearrage function." + ioe.getMessage();
                throw new ExecException(msg, errCode, ioe);
            }
        }
    }

    private void addKeyToBloomFilter(Object key) throws ExecException {
        Key k = new Key(DataType.toBytes(key, keyType));
        if (bloomFilters.length == 1) {
            if (bloomFilters[0] == null) {
                bloomFilters[0] = new BloomFilter(vectorSizeBytes * 8, numHash, hashType);
            }
            bloomFilters[0].add(k);
        } else {
            int partition = (key.hashCode() & Integer.MAX_VALUE) % numBloomFilters;
            BloomFilter filter = bloomFilters[partition];
            if (filter == null) {
                filter = new BloomFilter(vectorSizeBytes * 8, numHash, hashType);
                bloomFilters[partition] = filter;
            }
            filter.add(k);
        }
    }

    private void writeJoinKeyForBloom(Object key) throws IOException {
        int partition = (key.hashCode() & Integer.MAX_VALUE) % numBloomFilters;
        bloomValue.set(0, key);
        bloomWriter.write(new NullableIntWritable(partition), bloomNullableTuple);
    }

    private void writeBloomFilters() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(vectorSizeBytes + 64);
        for (int i = 0; i < bloomFilters.length; i++) {
            if (bloomFilters[i] != null) {
                DataOutputStream dos = new DataOutputStream(baos);
                bloomFilters[i].write(dos);
                dos.flush();
                bloomValue.set(0, new DataByteArray(baos.toByteArray()));
                bloomWriter.write(new NullableIntWritable(i), bloomNullableTuple);
                baos.reset();
            }
        }
    }

    @Override
    public POBuildBloomRearrangeTez clone() throws CloneNotSupportedException {
        return (POBuildBloomRearrangeTez) super.clone();
    }

    @Override
    public String name() {
        return getAliasString() + "BuildBloom Rearrange" + "["
                + DataType.findTypeName(resultType) + "]" + "{"
                + DataType.findTypeName(keyType) + "}" + "(" + mIsDistinct
                + ") - " + mKey.toString() + "\t->\t[ " + outputKey + ", " + bloomOutputKey +"]";
    }

}
