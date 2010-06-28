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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;

/**
 * A class of utility static methods to be used in the hadoop map reduce backend
 */
public class MapRedUtil {

    private static Log log = LogFactory.getLog(MapRedUtil.class);
         
    public static final String FILE_SYSTEM_NAME = "fs.default.name";

    /**
     * Loads the key distribution sampler file
     *
     * @param keyDistFile the name for the distribution file
     * @param totalReducers gets set to the total number of reducers as found in the dist file
     * @param keyType Type of the key to be stored in the return map. It currently treats Tuple as a special case.
     */
    @SuppressWarnings("unchecked")
    public static <E> Map<E, Pair<Integer, Integer>> loadPartitionFileFromLocalCache(
            String keyDistFile, Integer[] totalReducers, byte keyType)
            throws IOException {

        Map<E, Pair<Integer, Integer>> reducerMap = new HashMap<E, Pair<Integer, Integer>>();

        // use local file system to get the keyDistFile
        Configuration conf = new Configuration(false);            
        
        if (PigMapReduce.sJobConf.get("fs.file.impl")!=null)
            conf.set("fs.file.impl", PigMapReduce.sJobConf.get("fs.file.impl"));
        if (PigMapReduce.sJobConf.get("fs.hdfs.impl")!=null)
            conf.set("fs.hdfs.impl", PigMapReduce.sJobConf.get("fs.hdfs.impl"));
        conf.set(MapRedUtil.FILE_SYSTEM_NAME, "file:///");

        ReadToEndLoader loader = new ReadToEndLoader(new BinStorage(), conf, 
                keyDistFile, 0);
        DataBag partitionList;
        Tuple t = loader.getNext();
        if(t==null) {
            throw new RuntimeException("Empty samples file");
        }
        // The keydist file is structured as (key, min, max)
        // min, max being the index of the reducers
        Map<String, Object > distMap = (Map<String, Object>) t.get (0);
        partitionList = (DataBag) distMap.get(PartitionSkewedKeys.PARTITION_LIST);
        totalReducers[0] = Integer.valueOf(""+distMap.get(PartitionSkewedKeys.TOTAL_REDUCERS));
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
    
    public static void setupUDFContext(Configuration job) throws IOException {
        UDFContext udfc = UDFContext.getUDFContext();
        udfc.addJobConf(job);
        udfc.deserialize();
    }
    
    public static FileSpec checkLeafIsStore(
            PhysicalPlan plan,
            PigContext pigContext) throws ExecException {
        try {
            PhysicalOperator leaf = plan.getLeaves().get(0);
            FileSpec spec = null;
            if(!(leaf instanceof POStore)){
                String scope = leaf.getOperatorKey().getScope();
                POStore str = new POStore(new OperatorKey(scope,
                    NodeIdGenerator.getGenerator().getNextNodeId(scope)));
                spec = new FileSpec(FileLocalizer.getTemporaryPath(
                    pigContext).toString(),
                    new FuncSpec(BinStorage.class.getName()));
                str.setSFile(spec);
                plan.addAsLeaf(str);
            } else{
                spec = ((POStore)leaf).getSFile();
            }
            return spec;
        } catch (Exception e) {
            int errCode = 2045;
            String msg = "Internal error. Not able to check if the leaf node is a store operator.";
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    /**
     * Get all files recursively from the given list of files
     * 
     * @param files a list of FileStatus
     * @param conf the configuration object
     * @return the list of fileStatus that contains all the files in the given
     *         list and, recursively, all the files inside the directories in 
     *         the given list
     * @throws IOException
     */
    public static List<FileStatus> getAllFileRecursively(
            List<FileStatus> files, Configuration conf) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        int len = files.size();
        for (int i = 0; i < len; ++i) {
            FileStatus file = files.get(i);
            if (file.isDir()) {
                Path p = file.getPath();
                FileSystem fs = p.getFileSystem(conf);
                addInputPathRecursively(result, fs, p, hiddenFileFilter);
            } else {
                result.add(file);
            }
        }
        log.info("Total input paths to process : " + result.size()); 
        return result;
    }
    
    private static void addInputPathRecursively(List<FileStatus> result,
            FileSystem fs, Path path, PathFilter inputFilter) 
            throws IOException {
        for (FileStatus stat: fs.listStatus(path, inputFilter)) {
            if (stat.isDir()) {
                addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
            } else {
                result.add(stat);
            }
        }
    }          

    private static final PathFilter hiddenFileFilter = new PathFilter(){
        public boolean accept(Path p){
            String name = p.getName(); 
            return !name.startsWith("_") && !name.startsWith("."); 
        }
    };    
}
