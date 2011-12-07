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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.Collections;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

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
            String keyDistFile, Integer[] totalReducers, byte keyType, Configuration mapConf)
            throws IOException {

        Map<E, Pair<Integer, Integer>> reducerMap = new HashMap<E, Pair<Integer, Integer>>();

        // use local file system to get the keyDistFile
        Configuration conf = new Configuration(false);            
        
        if (mapConf.get("yarn.resourcemanager.principal")!=null) {
            conf.set("yarn.resourcemanager.principal", mapConf.get("yarn.resourcemanager.principal"));
        }
        
        if (PigMapReduce.sJobConfInternal.get().get("fs.file.impl")!=null)
            conf.set("fs.file.impl", PigMapReduce.sJobConfInternal.get().get("fs.file.impl"));
        if (PigMapReduce.sJobConfInternal.get().get("fs.hdfs.impl")!=null)
            conf.set("fs.hdfs.impl", PigMapReduce.sJobConfInternal.get().get("fs.hdfs.impl"));
        if (PigMapReduce.sJobConfInternal.get().getBoolean("pig.tmpfilecompression", false))
        {
            conf.setBoolean("pig.tmpfilecompression", true);
            if (PigMapReduce.sJobConfInternal.get().get("pig.tmpfilecompression.codec")!=null)
                conf.set("pig.tmpfilecompression.codec", PigMapReduce.sJobConfInternal.get().get("pig.tmpfilecompression.codec"));
        }
        conf.set(MapRedUtil.FILE_SYSTEM_NAME, "file:///");

        ReadToEndLoader loader = new ReadToEndLoader(Utils.getTmpFileStorageObject(PigMapReduce.sJobConfInternal.get()), conf, 
                keyDistFile, 0);
        DataBag partitionList;
        Tuple t = loader.getNext();
        if (t == null) {
            // this could happen if the input directory for sampling is empty
            log.warn("Empty dist file: " + keyDistFile);
            return reducerMap;
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
                Tuple keyTuple = TupleFactory.getInstance().newTuple();
                for (int i=0; i < idxTuple.size() - 2; i++) {
                    keyTuple.append(idxTuple.get(i));	
                }
                keyT = (E) keyTuple;
            } else {
                if (keyType == DataType.TUPLE) {
                    keyT = (E)TupleFactory.getInstance().newTuple(1);
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
        // don't deserialize in front-end 
        if (udfc.isUDFConfEmpty()) {
            udfc.deserialize();
        }
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
                    new FuncSpec(Utils.getTmpFileCompressorName(pigContext)));
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

    /* The following codes are for split combination: see PIG-1518
     * 
     */
    private static Comparator<Node> nodeComparator = new Comparator<Node>() {
        @Override
        public int compare(Node o1, Node o2) {
            long cmp = o1.length - o2.length;
            return cmp == 0 ? 0 : cmp < 0 ? -1 : 1;
        }
    };
    
    private static final class ComparableSplit implements Comparable<ComparableSplit> {
        private InputSplit rawInputSplit;
        private HashSet<Node> nodes;
        // id used as a tie-breaker when two splits are of equal size.
        private long id;
        ComparableSplit(InputSplit split, long id) {
            rawInputSplit = split;
            nodes = new HashSet<Node>();
            this.id = id;
        }
        
        void add(Node node) {
            nodes.add(node);
        }
        
        void removeFromNodes() {
            for (Node node : nodes)
                node.remove(this);
        }
        
        public InputSplit getSplit() {
            return rawInputSplit;
        }
  
        @Override
        public boolean equals(Object other) {
            if (other == null || !(other instanceof ComparableSplit))
                return false;
            return (compareTo((ComparableSplit) other) == 0);
        }
        
        @Override
        public int hashCode() {
            return 41;
        }
        
        @Override
        public int compareTo(ComparableSplit other) {
            try {
                long cmp = rawInputSplit.getLength() - other.rawInputSplit.getLength();
                // in descending order
                return cmp == 0 ? (id == other.id ? 0 : id < other.id ? -1 : 1) : cmp < 0 ?  1 : -1;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
      
    private static class DummySplit extends InputSplit {
        private long length;
        
        @Override
        public String[] getLocations() {
            return null;
        }
        
        @Override
        public long getLength() {
            return length;
        }
        
        public void setLength(long length) {
            this.length = length;
        }
    }
    
    private static class Node {
        private long length = 0;
        private ArrayList<ComparableSplit> splits;
        private boolean sorted;
        
        Node() throws IOException, InterruptedException {
            length = 0;
            splits = new ArrayList<ComparableSplit>();
            sorted = false;
        }
        
        void add(ComparableSplit split) throws IOException, InterruptedException {
            splits.add(split);
            length++;
        }
        
        void remove(ComparableSplit split) {
            if (!sorted)
                sort();
            int index = Collections.binarySearch(splits, split);
            if (index >= 0) {
                splits.remove(index);
                length--;
            }
        }
        
        void sort() {
            if (!sorted) {
                Collections.sort(splits);
                sorted = true;
            }
        }
        
        ArrayList<ComparableSplit> getSplits() {
            return splits;
        }
  
        public long getLength() {
            return length;
        }
    }
  
    public static List<List<InputSplit>> getCombinePigSplits(List<InputSplit>
        oneInputSplits, long maxCombinedSplitSize, Configuration conf)
          throws IOException, InterruptedException {
        ArrayList<Node> nodes = new ArrayList<Node>();
        HashMap<String, Node> nodeMap = new HashMap<String, Node>();
        List<List<InputSplit>> result = new ArrayList<List<InputSplit>>();
        List<Long> resultLengths = new ArrayList<Long>();
        long comparableSplitId = 0;
        
        int size = 0, nSplits = oneInputSplits.size();
        InputSplit lastSplit = null;
        int emptyCnt = 0;
        for (InputSplit split : oneInputSplits) {
            if (split.getLength() == 0) {
                emptyCnt++; 
                continue;
            }
            if (split.getLength() >= maxCombinedSplitSize) {
                comparableSplitId++;
                ArrayList<InputSplit> combinedSplits = new ArrayList<InputSplit>();
                combinedSplits.add(split);
                result.add(combinedSplits);
                resultLengths.add(split.getLength());
            } else {
                ComparableSplit csplit = new ComparableSplit(split, comparableSplitId++);
                String[] locations = split.getLocations();
                // sort the locations to stabilize the number of maps: PIG-1757
                Arrays.sort(locations);
                HashSet<String> locationSeen = new HashSet<String>();
                for (String location : locations)
                {
                    if (!locationSeen.contains(location)) 
                    {
                        Node node = nodeMap.get(location);
                        if (node == null) {
                            node = new Node();
                            nodes.add(node);
                            nodeMap.put(location, node);
                        }
                        node.add(csplit);
                        csplit.add(node);
                        locationSeen.add(location);
                    }
                }
                lastSplit = split;
                size++;
            }
        }
        /* verification code: debug purpose
        {
          ArrayList<ComparableSplit> leftoverSplits = new ArrayList<ComparableSplit>();
          HashSet<InputSplit> seen = new HashSet<InputSplit>();
          for (Node node : nodes) {
            if (node.getLength() > 0)
            {
              ArrayList<ComparableSplit> splits = node.getSplits();
              for (ComparableSplit split : splits) {
                if (!seen.contains(split.getSplit())) {
                  // remove duplicates. The set has to be on the raw input split not the 
                  // comparable input split as the latter overrides the compareTo method
                  // so its equality semantics is changed and not we want here
                  seen.add(split.getSplit());
                  leftoverSplits.add(split);
                }
              }
            }
          }
          
          int combinedSplitLen = 0;
          for (PigSplit split : result)
            combinedSplitLen += split.getNumPaths();
          if (combinedSplitLen + leftoverSplits.size()!= nSplits-emptyCnt) {
            throw new AssertionError("number of combined splits {"+combinedSplitLen+"+"+leftoverSplits.size()+"-"+size+"} does not match the number of original splits ["+nSplits+"].");
          }
        }
        */
        if (nSplits > 0 && emptyCnt == nSplits)
        {
            // if all splits are empty, add a single empty split as currently an empty directory is
            // not properly handled somewhere
            ArrayList<InputSplit> combinedSplits = new ArrayList<InputSplit>();
            combinedSplits.add(oneInputSplits.get(0));
            result.add(combinedSplits);
        }
        else if (size == 1) {
            ArrayList<InputSplit> combinedSplits = new ArrayList<InputSplit>();
            combinedSplits.add(lastSplit);
            result.add(combinedSplits);
        } else if (size > 1) {
            // combine small splits
            Collections.sort(nodes, nodeComparator);
            DummySplit dummy = new DummySplit();
            // dummy is used to search for next split of suitable size to be combined
            ComparableSplit dummyComparableSplit = new ComparableSplit(dummy, -1);
            for (Node node : nodes) {
                // sort the splits on this node in descending order
                node.sort();
                long totalSize = 0;
                ArrayList<ComparableSplit> splits = node.getSplits();
                int idx;
                int lenSplits;
                ArrayList<InputSplit> combinedSplits = new ArrayList<InputSplit>();
                ArrayList<ComparableSplit> combinedComparableSplits = new ArrayList<ComparableSplit>();
                while (!splits.isEmpty()) {
                    combinedSplits.add(splits.get(0).getSplit());
                    combinedComparableSplits.add(splits.get(0));
                    int startIdx = 1;
                    lenSplits = splits.size();
                    totalSize += splits.get(0).getSplit().getLength();
                    long spaceLeft = maxCombinedSplitSize - totalSize;
                    dummy.setLength(spaceLeft);
                    idx = Collections.binarySearch(node.getSplits().subList(startIdx, lenSplits), dummyComparableSplit);
                    idx = -idx-1+startIdx;
                    while (idx < lenSplits)
                    {
                        long thisLen = splits.get(idx).getSplit().getLength();
                        combinedSplits.add(splits.get(idx).getSplit());
                        combinedComparableSplits.add(splits.get(idx));
                        totalSize += thisLen;
                        spaceLeft -= thisLen;
                        if (spaceLeft <= 0)
                            break;
                        // find next combinable chunk
                        startIdx = idx + 1;
                        if (startIdx >= lenSplits)
                            break;
                        dummy.setLength(spaceLeft);
                        idx = Collections.binarySearch(node.getSplits().subList(startIdx, lenSplits), dummyComparableSplit);
                        idx = -idx-1+startIdx;
                    }
                    if (totalSize > maxCombinedSplitSize/2) {
                        result.add(combinedSplits);
                        resultLengths.add(totalSize);
                        removeSplits(combinedComparableSplits);
                        totalSize = 0;
                        combinedSplits = new ArrayList<InputSplit>();
                        combinedComparableSplits.clear();
                        splits = node.getSplits();
                    } else {
                        if (combinedSplits.size() != lenSplits)
                            throw new AssertionError("Combined split logic error!");
                        break;
                    }
                }
            }
            // handle leftovers
            ArrayList<ComparableSplit> leftoverSplits = new ArrayList<ComparableSplit>();
            HashSet<InputSplit> seen = new HashSet<InputSplit>();
            for (Node node : nodes) {
                for (ComparableSplit split : node.getSplits()) {
                    if (!seen.contains(split.getSplit())) {
                        // remove duplicates. The set has to be on the raw input split not the 
                        // comparable input split as the latter overrides the compareTo method
                        // so its equality semantics is changed and not we want here
                        seen.add(split.getSplit());
                        leftoverSplits.add(split);
                    }
                }
            }
            
            /* verification code
            int combinedSplitLen = 0;
            for (PigSplit split : result)
              combinedSplitLen += split.getNumPaths();
            if (combinedSplitLen + leftoverSplits.size()!= nSplits-emptyCnt)
              throw new AssertionError("number of combined splits ["+combinedSplitLen+"+"+leftoverSplits.size()+"] does not match the number of original splits ["+nSplits+"].");
            */
            if (!leftoverSplits.isEmpty())
            {
                long totalSize = 0;
                ArrayList<InputSplit> combinedSplits = new ArrayList<InputSplit>();
                ArrayList<ComparableSplit> combinedComparableSplits = new ArrayList<ComparableSplit>();
                
                int splitLen = leftoverSplits.size();
                for (int i = 0; i < splitLen; i++)
                {
                    ComparableSplit split = leftoverSplits.get(i);
                    long thisLen = split.getSplit().getLength();
                    if (totalSize + thisLen >= maxCombinedSplitSize) {
                        removeSplits(combinedComparableSplits);
                        result.add(combinedSplits);
                        resultLengths.add(totalSize);
                        combinedSplits = new ArrayList<InputSplit>();
                        combinedComparableSplits.clear();
                        totalSize = 0;
                    }
                    combinedSplits.add(split.getSplit());
                    combinedComparableSplits.add(split);
                    totalSize += split.getSplit().getLength();
                    if (i == splitLen - 1) {
                        // last piece: it could be very small, try to see it can be squeezed into any existing splits
                        for (int j =0; j < result.size(); j++)
                        {
                            if (resultLengths.get(j) + totalSize <= maxCombinedSplitSize)
                            {
                                List<InputSplit> isList = result.get(j);
                                for (InputSplit csplit : combinedSplits) {
                                    isList.add(csplit);
                                }
                                removeSplits(combinedComparableSplits);
                                combinedSplits.clear();
                                break;
                            }
                        }
                        if (!combinedSplits.isEmpty()) {
                            // last piece can not be squeezed in, create a new combined split for them.
                            removeSplits(combinedComparableSplits);
                            result.add(combinedSplits);
                        }
                    }
                }
            }
        }
        /* verification codes
        int combinedSplitLen = 0;
        for (PigSplit split : result)
          combinedSplitLen += split.getNumPaths();
        if (combinedSplitLen != nSplits-emptyCnt)
          throw new AssertionError("number of combined splits ["+combinedSplitLen+"] does not match the number of original splits ["+nSplits+"].");
        
        long totalLen = 0;
        for (PigSplit split : result)
          totalLen += split.getLength();
        
        long origTotalLen = 0;
        for (InputSplit split : oneInputSplits)
          origTotalLen += split.getLength();
        if (totalLen != origTotalLen)
          throw new AssertionError("The total length ["+totalLen+"] does not match the original ["+origTotalLen+"]");
        */ 
        log.info("Total input paths (combined) to process : " + result.size());
        return result;
    }
    
    private static void removeSplits(List<ComparableSplit> splits) {
        for (ComparableSplit split: splits)
            split.removeFromNodes();
    }
    
    public String inputSplitToString(InputSplit[] splits) throws IOException, InterruptedException {
        // debugging purpose only
        StringBuilder st = new StringBuilder();
        st.append("Number of splits :" + splits.length+"\n");
        long len = 0;
        for (InputSplit split: splits)
            len += split.getLength();
        st.append("Total Length = "+ len +"\n");
        for (int i = 0; i < splits.length; i++) {
            st.append("Input split["+i+"]:\n   Length = "+ splits[i].getLength()+"\n  Locations:\n");
            for (String location :  splits[i].getLocations())
                st.append("    "+location+"\n");
            st.append("\n-----------------------\n"); 
        }
        return st.toString();
    }
    
    /* verification code: debug purpose only
    public String inputSplitToString(ArrayList<ComparableSplit> splits) throws IOException, InterruptedException {
      StringBuilder st = new StringBuilder();
      st.append("Number of splits :" + splits.size()+"\n");
      long len = 0;
      for (ComparableSplit split: splits)
        len += split.getSplit().getLength();
      st.append("Total Length = "+ len +"\n");
      for (int i = 0; i < splits.size(); i++) {
        st.append("Input split["+i+"]:\n   Length = "+ splits.get(i).getSplit().getLength()+"\n  Locations:\n");
        for (String location :  splits.get(i).getSplit().getLocations())
          st.append("    "+location+"\n");
        st.append("\n-----------------------\n"); 
      }
      return st.toString();
    }
    */
}
