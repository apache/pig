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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.FuncSpec;
import org.apache.pig.IndexableLoadFunc;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.SeekableInputStream;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.tools.bzip2r.CBZip2InputStream;

/**
 *
 */
//XXX FIXME - make this work with new load-store redesign
public class DefaultIndexableLoader extends IndexableLoadFunc {

    
    // FileSpec of index file which will be read from HDFS.
    private String indexFile;
    private String indexFileLoadFuncSpec;
    
    private LoadFunc loader;
    // Index is modeled as FIFO queue and LinkedList implements java Queue interface.  
    private LinkedList<Tuple> index;
    private FuncSpec rightLoaderFuncSpec;
    private PigContext pc;
    private String scope;
    private Tuple dummyTuple = null;
    private transient TupleFactory mTupleFactory;
    private InputStream  is;
    private String currentFileName;
    
    public DefaultIndexableLoader(String loaderFuncSpec, String indexFile, String indexFileLoadFuncSpec, String scope) {
        this.rightLoaderFuncSpec = new FuncSpec(loaderFuncSpec);
        this.indexFile = indexFile;
        this.indexFileLoadFuncSpec = indexFileLoadFuncSpec;
        this.scope = scope;
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
        
        POLoad ld = new POLoad(genKey(), new FileSpec(indexFile, new FuncSpec(indexFileLoadFuncSpec)), false);
        try {
            pc = (PigContext)ObjectSerializer.deserialize(PigMapReduce.sJobConf.get("pig.pigContext"));
        } catch (IOException e) {
            int errCode = 2094;
            String msg = "Unable to deserialize pig context.";
            throw new ExecException(msg,errCode,e);
        }
        pc.connect();
        ld.setPc(pc);
        index = new LinkedList<Tuple>();
        for(Result res=ld.getNext(dummyTuple);res.returnStatus!=POStatus.STATUS_EOP;res=ld.getNext(dummyTuple))
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
                break;
            }
            Object extractedKey = extractKeysFromIdxTuple(curIdxEntry);
            if(extractedKey == null){
                prevIdxEntry = curIdxEntry;
                continue;
            }
            
            if(((Comparable)extractedKey).compareTo(firstLeftKey) >= 0){
                if(null == prevIdxEntry)   // very first entry in index.
                    matchedEntry = curIdxEntry;
                else{
                    matchedEntry = prevIdxEntry;
                    index.addFirst(curIdxEntry);  // We need to add back the current index Entry because we are reading ahead.
                }
                break;
            }
            else
                prevIdxEntry = curIdxEntry;
        }

        if(matchedEntry == null){
            
            int errCode = 2165;
            String errMsg = "Problem in index construction.";
            throw new ExecException(errMsg,errCode,PigException.BUG);
        }
        
        Object extractedKey = extractKeysFromIdxTuple(matchedEntry);
        
        if(extractedKey != null){
            Class idxKeyClass = extractedKey.getClass();
            if( ! firstLeftKey.getClass().equals(idxKeyClass)){

                // This check should indeed be done on compile time. But to be on safe side, we do it on runtime also.
                int errCode = 2166;
                String errMsg = "Key type mismatch. Found key of type "+firstLeftKey.getClass().getCanonicalName()+" on left side. But, found key of type "+ idxKeyClass.getCanonicalName()+" in index built for right side.";
                throw new ExecException(errMsg,errCode,PigException.BUG);
            }
        }
        initRightLoader(matchedEntry);
    }
    
    private void initRightLoader(Tuple idxEntry) throws IOException{

        // bind loader to file pointed by this index Entry.
        int keysCnt = idxEntry.size();
        Long offset = (Long)idxEntry.get(keysCnt-1);
        if(offset > 0)
            // Loader will throw away one tuple if we are in the middle of the block. We don't want that.
            offset -= 1 ;
        FileSpec lFile = new FileSpec((String)idxEntry.get(keysCnt-2),this.rightLoaderFuncSpec);
        currentFileName = lFile.getFileName();
        loader = (LoadFunc)PigContext.instantiateFuncFromSpec(lFile.getFuncSpec());
        is = FileLocalizer.open(currentFileName, offset, pc);
        if (currentFileName.endsWith(".bz") || currentFileName.endsWith(".bz2")) {
            is = new CBZip2InputStream((SeekableInputStream)is, 9);
        } else if (currentFileName.endsWith(".gz")) {
            is = new GZIPInputStream(is);
        }

        
//        loader.bindTo(currentFileName , new BufferedPositionedInputStream(is), offset, Long.MAX_VALUE);
    }

    private Object extractKeysFromIdxTuple(Tuple idxTuple) throws ExecException{

        int idxTupSize = idxTuple.size();

        if(idxTupSize == 3)
            return idxTuple.get(0);
        
        List<Object> list = new ArrayList<Object>(idxTupSize-2);
        for(int i=0; i<idxTupSize-2;i++)
            list.add(idxTuple.get(i));

        return mTupleFactory.newTupleNoCopy(list);
    }

    private OperatorKey genKey(){
        return new OperatorKey(scope,NodeIdGenerator.getGenerator().getNextNodeId(scope));
    }
    
    @Override
    public Tuple getNext() throws IOException {
        Tuple t = loader.getNext();
        if(t == null) {
            while(true){                        // But next file may be same as previous one, because index may contain multiple entries for same file.
                Tuple idxEntry = index.poll();
                if(null == idxEntry) {           // Index is finished too. Right stream is finished. No more tuples.
                    return null;
                } else {                           
                    if(currentFileName.equals((String)idxEntry.get(idxEntry.size()-2))) {
                        continue;
                    } else {
                        initRightLoader(idxEntry);      // bind loader to file and get tuple from it.
                        return loader.getNext();    
                    }
                }
           }
        }
        return t;
    }
    
    @Override
    public void close() throws IOException {
        is.close();
    }

    @Override
    public void initialize(Configuration conf) throws IOException {
        // nothing to do
        
    }

    @Override
    public InputFormat getInputFormat() {
        return null;
    }
    
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {        
    }
    
}
