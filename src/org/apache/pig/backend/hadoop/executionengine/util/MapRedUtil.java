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

package org.apache.pig.backend.hadoop.executionengine.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.pig.PigException;
import org.apache.pig.StoreConfig;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;

import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.io.NullablePartitionWritable;

import org.apache.pig.impl.util.Pair;
import org.apache.pig.data.DefaultTupleFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.data.Tuple;
import java.io.InputStream;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DataType;

/**
 * A class of utility static methods to be used in the hadoop map reduce backend
 */
public class MapRedUtil {

    /**
     * This method is to be called from an 
     * {@link org.apache.hadoop.mapred.OutputFormat#getRecordWriter(FileSystem ignored, JobConf job,
                                     String name, Progressable progress)}
     * method to obtain a reference to the {@link org.apache.pig.StoreFunc} object to be used by
     * that OutputFormat to perform the write() operation
     * @param conf the JobConf object
     * @return the StoreFunc reference
     * @throws ExecException
     */
    public static StoreFunc getStoreFunc(JobConf conf) throws ExecException {
        StoreFunc store;
        try {
            String storeFunc = conf.get("pig.storeFunc", "");
            if (storeFunc.length() == 0) {
                store = new PigStorage();
            } else {
                storeFunc = (String) ObjectSerializer.deserialize(storeFunc);
                store = (StoreFunc) PigContext
                        .instantiateFuncFromSpec(storeFunc);
            }
        } catch (Exception e) {
            int errCode = 2081;
            String msg = "Unable to setup the store function.";
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
        return store;
    }
    
    /**
     * This method is to be called from an 
     * {@link org.apache.hadoop.mapred.OutputFormat#getRecordWriter(FileSystem ignored, JobConf job,
                                     String name, Progressable progress)}
     * method to obtain a reference to the {@link org.apache.pig.StoreConfig} object. The StoreConfig
     * object will contain metadata information like schema and location to be used by
     * that OutputFormat to perform the write() operation
     * @param conf the JobConf object
     * @return StoreConfig object containing metadata information useful for
     * an OutputFormat to write the data
     * @throws IOException
     */
    public static StoreConfig getStoreConfig(JobConf conf) throws IOException {
        return (StoreConfig) ObjectSerializer.deserialize(conf.get(JobControlCompiler.PIG_STORE_CONFIG));
    }

	/**
	 * Loads the key distribution sampler file
     *
     * @param keyDistFile the name for the distribution file
     * @param totalReducers gets set to the total number of reducers as found in the dist file
     * @param job Ref to a jobCong object
     * @param keyType Type of the key to be stored in the return map. It currently treats Tuple as a special case.
	 */	
	@SuppressWarnings("unchecked")
	public static <E> Map<E, Pair<Integer, Integer> > loadPartitionFile(String keyDistFile,
								 Integer[] totalReducers, JobConf job, byte keyType) throws IOException {

		Map<E, Pair<Integer, Integer> > reducerMap = new HashMap<E, Pair<Integer, Integer> >();
		
		InputStream is;
		if (job != null) {
			is = FileLocalizer.openDFSFile(keyDistFile,ConfigurationUtil.toProperties(job));
		} else {
			is = FileLocalizer.openDFSFile(keyDistFile);
		}
		BinStorage loader = new BinStorage();
		DataBag partitionList;
		loader.bindTo(keyDistFile, new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
		Tuple t = loader.getNext();
		if(t==null) {
			throw new RuntimeException("Empty samples file");
		}
		// The keydist file is structured as (key, min, max)
		// min, max being the index of the reducers
		Map<String, Object > distMap = (Map<String, Object>) t.get (0);
		partitionList = (DataBag) distMap.get("partition.list");
		totalReducers[0] = Integer.valueOf(""+distMap.get("totalreducers"));
		Iterator<Tuple> it = partitionList.iterator();
		while (it.hasNext()) {
			Tuple idxTuple = it.next();
			Integer maxIndex = (Integer) idxTuple.get(idxTuple.size() - 1);
			Integer minIndex = (Integer) idxTuple.get(idxTuple.size() - 2);
			// Used to replace the maxIndex with the number of reducers
			if (maxIndex < minIndex) {
				maxIndex = totalReducers[0] + maxIndex; 
			}
			E keyT;

			// if the join is on more than 1 key
			if (idxTuple.size() > 3) {
				// remove the last 2 fields of the tuple, i.e: minIndex and maxIndex and store
				// it in the reducer map
				Tuple keyTuple = DefaultTupleFactory.getInstance().newTuple();
				for (int i=0; i < idxTuple.size() - 2; i++) {
					keyTuple.append(idxTuple.get(i));	
				}
				keyT = (E) keyTuple;
			} else {
				if (keyType == DataType.TUPLE) {
					keyT = (E)DefaultTupleFactory.getInstance().newTuple(1);
					((Tuple)keyT).set(0,idxTuple.get(0));
				} else {
					keyT = (E) idxTuple.get(0);
				}
			}
			// number of reducers
			Integer cnt = maxIndex - minIndex;
			reducerMap.put(keyT, new Pair(minIndex, cnt));// 1 is added to account for the 0 index
		}		
		return reducerMap;
	}
}
