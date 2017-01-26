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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.ObjectCache;
import org.apache.pig.backend.hadoop.executionengine.tez.runtime.TezInput;
import org.apache.pig.builtin.BuildBloomBase;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class POBloomFilterRearrangeTez extends POLocalRearrangeTez implements TezInput {
    private static final long serialVersionUID = 1L;

    private static final Log LOG = LogFactory.getLog(POBloomFilterRearrangeTez.class);
    private String inputKey;
    private transient KeyValueReader reader;
    private transient String cacheKey;
    private int numBloomFilters;
    private transient BloomFilter[] bloomFilters;

    public POBloomFilterRearrangeTez(POLocalRearrangeTez lr, int numBloomFilters) {
        super(lr);
        this.numBloomFilters = numBloomFilters;
    }

    public void setInputKey(String inputKey) {
        this.inputKey = inputKey;
    }

    @Override
    public String[] getTezInputs() {
        return new String[] { inputKey };
    }

    @Override
    public void replaceInput(String oldInputKey, String newInputKey) {
        if (oldInputKey.equals(inputKey)) {
            inputKey = newInputKey;
        }
    }

    @Override
    public void addInputsToSkip(Set<String> inputsToSkip) {
        cacheKey = "bloom-" + inputKey;
        Object cacheValue = ObjectCache.getInstance().retrieve(cacheKey);
        if (cacheValue != null) {
            inputsToSkip.add(inputKey);
        }
    }

    @Override
    public void attachInputs(Map<String, LogicalInput> inputs,
            Configuration conf) throws ExecException {
        Object cacheValue = ObjectCache.getInstance().retrieve(cacheKey);
        if (cacheValue != null) {
            bloomFilters = (BloomFilter[]) cacheValue;
            return;
        }
        LogicalInput input = inputs.get(inputKey);
        if (input == null) {
            throw new ExecException("Input from vertex " + inputKey + " is missing");
        }
        try {
            reader = (KeyValueReader) input.getReader();
            LOG.info("Attached input from vertex " + inputKey + " : input=" + input + ", reader=" + reader);
            while (reader.next()) {
                if (bloomFilters == null) {
                    bloomFilters = new BloomFilter[numBloomFilters];
                }
                Tuple val = (Tuple) reader.getCurrentValue();
                int index = (int) val.get(0);
                bloomFilters[index] = BuildBloomBase.bloomIn((DataByteArray) val.get(1));
            }
            ObjectCache.getInstance().cache(cacheKey, bloomFilters);
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Result getNextTuple() throws ExecException {

        // If there is no bloom filter, then it means right input was empty
        // Skip processing
        if (bloomFilters == null) {
            return RESULT_EOP;
        }

        while (true) {
            res = super.getRearrangedTuple();
            try {
                switch (res.returnStatus) {
                case POStatus.STATUS_OK:
                    if (illustrator == null) {
                        Tuple result = (Tuple) res.result;
                        Byte index = (Byte) result.get(0);

                        // Skip the record if key is not in the bloom filter
                        if (!isKeyInBloomFilter(result.get(1))) {
                            continue;
                        }
                        PigNullableWritable key = HDataType.getWritableComparableTypes(result.get(1), keyType);
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
                case POStatus.STATUS_ERR:
                default:
                    return res;
                }
            } catch (IOException ioe) {
                int errCode = 2135;
                String msg = "Received error from POBloomFilterRearrage function." + ioe.getMessage();
                throw new ExecException(msg, errCode, ioe);
            }
        }
    }

    private boolean isKeyInBloomFilter(Object key) throws ExecException {
        if (key == null) {
            // Null values are dropped in a inner join and in the case of outer join,
            // POBloomFilterRearrangeTez is only in the plan on the non outer relation.
            // So just skip them
            return false;
        }
        if (bloomFilters.length == 1) {
            // Skip computing hashcode
            Key k = new Key(DataType.toBytes(key, keyType));
            return bloomFilters[0].membershipTest(k);
        } else {
            int partition = (key.hashCode() & Integer.MAX_VALUE) % numBloomFilters;
            BloomFilter filter = bloomFilters[partition];
            if (filter != null) {
                Key k = new Key(DataType.toBytes(key, keyType));
                return filter.membershipTest(k);
            }
            return false;
        }
    }

    @Override
    public POBloomFilterRearrangeTez clone() throws CloneNotSupportedException {
        return (POBloomFilterRearrangeTez) super.clone();
    }

    @Override
    public String name() {
        return getAliasString() + "BloomFilter Rearrange" + "["
                + DataType.findTypeName(resultType) + "]" + "{"
                + DataType.findTypeName(keyType) + "}" + "(" + mIsDistinct
                + ") - " + mKey.toString() + "\t<-\t " + inputKey + "\t->\t " + outputKey;
    }

}
