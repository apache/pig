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
package org.apache.pig.impl.builtin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.IndexableLoadFunc;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;



/**
 * Used by MergeJoin . Takes an index on sorted data
 * consisting of sorted tuples of the form
 * (key1,key2..., position,splitIndex) as input. For key given in seekNear(Tuple)
 * finds the splitIndex that can contain the key and initializes ReadToEndLoader
 * to read from that splitIndex onwards , in the sequence of splits in the index
 */
public class DefaultIndexableLoader extends LoadFunc implements IndexableLoadFunc{

    private static final Log LOG = LogFactory.getLog(DefaultIndexableLoader.class);
    
    // FileSpec of index file which will be read from HDFS.
    private String indexFile;
    private String indexFileLoadFuncSpec;
    
    private LoadFunc loader;
    // Index is modeled as FIFO queue and LinkedList implements java Queue interface.  
    private LinkedList<Tuple> index;
    private FuncSpec rightLoaderFuncSpec;

    private String scope;
    private Tuple dummyTuple = null;
    private transient TupleFactory mTupleFactory;

    private String inpLocation;
    
    public DefaultIndexableLoader(
            String loaderFuncSpec, 
            String indexFile,
            String indexFileLoadFuncSpec,
            String scope,
            String inputLocation
    ) {
        this.rightLoaderFuncSpec = new FuncSpec(loaderFuncSpec);
        this.indexFile = indexFile;
        this.indexFileLoadFuncSpec = indexFileLoadFuncSpec;
        this.scope = scope;
        this.inpLocation = inputLocation;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void seekNear(Tuple keys) throws IOException{
        // some setup
        mTupleFactory = TupleFactory.getInstance();

        /* Currently whole of index is read into memory. Typically, index is small. Usually 
           few KBs in size. So, this should not be an issue.
           However, reading whole index at startup time is not required. So, this can be improved upon.
           Assumption: Index being read is sorted on keys followed by filename, followed by offset.
         */

        // Index is modeled as FIFO Queue, that frees us from keeping track of which index entry should be read next.
        
        // the keys are sent in a tuple. If there is really only
        // 1 join key, it would be the first field of the tuple. If
        // there are multiple Join keys, the tuple itself represents
        // the join key
        Object firstLeftKey = (keys.size() == 1 ? keys.get(0): keys);
        POLoad ld = new POLoad(genKey(), new FileSpec(indexFile, new FuncSpec(indexFileLoadFuncSpec)));
                
        Properties props = ConfigurationUtil.getLocalFSProperties();
        PigContext pc = new PigContext(ExecType.LOCAL, props);
        ld.setPc(pc);
        index = new LinkedList<Tuple>();
        for(Result res=ld.getNextTuple();res.returnStatus!=POStatus.STATUS_EOP;res=ld.getNextTuple())
            index.offer((Tuple) res.result);   

        
        Tuple prevIdxEntry = null;
        Tuple matchedEntry;
     
        // When the first call is made, we need to seek into right input at correct offset.
        while(true){
            // Keep looping till we find first entry in index >= left key
            // then return the prev idx entry.

            Tuple curIdxEntry = index.poll();
            if(null == curIdxEntry){
                // Its possible that we hit end of index and still doesn't encounter
                // idx entry >= left key, in that case return last index entry.
                matchedEntry = prevIdxEntry;
                if (prevIdxEntry!=null) {
                    Object extractedKey = extractKeysFromIdxTuple(prevIdxEntry);
                    if (extractedKey!=null)
                        index.add(prevIdxEntry);
                }
                break;
            }
            Object extractedKey = extractKeysFromIdxTuple(curIdxEntry);
            if(extractedKey == null){
                prevIdxEntry = curIdxEntry;
                continue;
            }
            
            if(((Comparable)extractedKey).compareTo(firstLeftKey) >= 0){
                index.addFirst(curIdxEntry);  // We need to add back the current index Entry because we are reading ahead.
                if(null == prevIdxEntry)   // very first entry in index.
                    matchedEntry = curIdxEntry;
                else{
                    matchedEntry = prevIdxEntry; 
                    // start join from previous idx entry, it might have tuples
                    // with this key                    
                    index.addFirst(prevIdxEntry);
                }
                break;
            }
            else
                prevIdxEntry = curIdxEntry;
        }

        if (matchedEntry == null) {
            LOG.warn("Empty index file: input directory is empty");
        } else {
        
            Object extractedKey = extractKeysFromIdxTuple(matchedEntry);
            
            if (extractedKey != null) {
                Class idxKeyClass = extractedKey.getClass();
                if( ! firstLeftKey.getClass().equals(idxKeyClass)){
    
                    // This check should indeed be done on compile time. But to be on safe side, we do it on runtime also.
                    int errCode = 2166;
                    String errMsg = "Key type mismatch. Found key of type "+firstLeftKey.getClass().getCanonicalName()+" on left side. But, found key of type "+ idxKeyClass.getCanonicalName()+" in index built for right side.";
                    throw new ExecException(errMsg,errCode,PigException.BUG);
                }
            } 
        }
        
        //add remaining split indexes to splitsAhead array
        int [] splitsAhead = new int[index.size()];
        int splitsAheadIdx = 0;
        for(Tuple t : index){
            splitsAhead[splitsAheadIdx++] = (Integer) t.get( t.size()-1 );
        }
        
        initRightLoader(splitsAhead);
    }
    
    private void initRightLoader(int [] splitsToBeRead) throws IOException{
        PigContext pc = (PigContext) ObjectSerializer
                .deserialize(PigMapReduce.sJobConfInternal.get().get("pig.pigContext"));
        
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        
        // Hadoop security need this property to be set
        if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            conf.set("mapreduce.job.credentials.binary", 
                    System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }
        
        //create ReadToEndLoader that will read the given splits in order
        loader = new ReadToEndLoader((LoadFunc)PigContext.instantiateFuncFromSpec(rightLoaderFuncSpec),
                conf, inpLocation, splitsToBeRead);
    }

    private Object extractKeysFromIdxTuple(Tuple idxTuple) throws ExecException{

        int idxTupSize = idxTuple.size();

        if(idxTupSize == 3)
            return idxTuple.get(0);
        
        int numColsInKey = (idxTupSize - 2);
        List<Object> list = new ArrayList<Object>(numColsInKey);
        for(int i=0; i < numColsInKey; i++)
            list.add(idxTuple.get(i));

        return mTupleFactory.newTupleNoCopy(list);
    }

    private OperatorKey genKey(){
        return new OperatorKey(scope,NodeIdGenerator.getGenerator().getNextNodeId(scope));
    }
    
    @Override
    public Tuple getNext() throws IOException {
        Tuple t = loader.getNext();
        return t;
    }
    
    @Override
    public void close() throws IOException {
    }

    @Override
    public void initialize(Configuration conf) throws IOException {
        // nothing to do
        
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public LoadCaster getLoadCaster() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        // nothing to do
    }
    
    public void setIndexFile(String indexFile) {
        this.indexFile = indexFile;
    }

}
