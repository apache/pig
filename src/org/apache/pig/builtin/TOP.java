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
package org.apache.pig.builtin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Top UDF accepts a bag of tuples and returns top-n tuples depending upon the
 * tuple field value of type long. Both n and field number needs to be provided
 * to the UDF. The UDF iterates through the input bag and just retains top-n
 * tuples by storing them in a priority queue of size n+1 where priority is the
 * long field. This is efficient as priority queue provides constant time - O(1)
 * removal of the least element and O(log n) time for heap restructuring. The
 * UDF is especially helpful for turning the nested grouping operation inside
 * out and retaining top-n in a nested group. 
 * 
 * Assumes all tuples in the bag contain an element of the same type in the compared column.
 * 
 * Sample usage: 
 * A = LOAD 'test.tsv' as (first: chararray, second: chararray); 
 * B = GROUP A BY (first, second);
 * C = FOREACH B generate FLATTEN(group), COUNT(*) as count;
 * D = GROUP C BY first; // again group by first 
 * topResults = FOREACH D { 
 *          result = Top(10, 1, C); // and retain top 10 occurrences of 'second' in first 
 *          GENERATE FLATTEN(result); 
 *  }
 */
public class TOP extends EvalFunc<DataBag> implements Algebraic{
    private static final Log log = LogFactory.getLog(TOP.class);
    static BagFactory mBagFactory = BagFactory.getInstance();
    static TupleFactory mTupleFactory = TupleFactory.getInstance();
    private Random randomizer = new Random();

    static class TupleComparator implements Comparator<Tuple> {
        private final int fieldNum;
        private byte datatype;
        private boolean typeFound=false;

        public TupleComparator(int fieldNum) {
            this.fieldNum = fieldNum;
        }

        /*          
         * (non-Javadoc)
         * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
         */
        @Override
        public int compare(Tuple o1, Tuple o2) {
            if (o1 == null)
                return -1;
            if (o2 == null)
                return 1;
            try {
                Object field1 = o1.get(fieldNum);
                Object field2 = o2.get(fieldNum);
                if (!typeFound) {
                    datatype = DataType.findType(field1);
                    if(datatype != DataType.NULL) {
                        typeFound = true;
                    }
                }
                return DataType.compare(field1, field2, datatype, datatype);
            } catch (ExecException e) {
                throw new RuntimeException("Error while comparing o1:" + o1
                        + " and o2:" + o2, e);
            }
        }
    }

    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() < 3) {
            return null;
        }
        try {
            int n = (Integer) tuple.get(0);
            int fieldNum = (Integer) tuple.get(1);
            DataBag inputBag = (DataBag) tuple.get(2);
            if (inputBag == null) {
                return null;
            }
            PriorityQueue<Tuple> store = new PriorityQueue<Tuple>(n + 1,
                    new TupleComparator(fieldNum));
            updateTop(store, n, inputBag);
            DataBag outputBag = mBagFactory.newDefaultBag();
            for (Tuple t : store) {
                outputBag.add(t);
            }
            if (log.isDebugEnabled()) {
                if (randomizer.nextInt(1000) == 1) {
                    log.debug("outputting a bag: ");
                    for (Tuple t : outputBag) 
                        log.debug("outputting "+t.toDelimitedString("\t"));
                    log.debug("==================");
                }
            }
            return outputBag;
        } catch (ExecException e) {
            throw new RuntimeException("ExecException executing function: ", e);
        } catch (Exception e) {
            throw new RuntimeException("General Exception executing function: ", e);
        }
    }

    protected static void updateTop(PriorityQueue<Tuple> store, int limit, DataBag inputBag) {
        Iterator<Tuple> itr = inputBag.iterator();
        while (itr.hasNext()) {
            Tuple t = itr.next();
            store.add(t);
            if (store.size() > limit)
                store.poll();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FieldSchema> fields = new ArrayList<FieldSchema>(3);
        fields.add(new Schema.FieldSchema(null, DataType.INTEGER));
        fields.add(new Schema.FieldSchema(null, DataType.INTEGER));
        fields.add(new Schema.FieldSchema(null, DataType.BAG));
        FuncSpec funcSpec = new FuncSpec(this.getClass().getName(), new Schema(fields));
        List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>(1);
        funcSpecs.add(funcSpec);
        return funcSpecs;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            if (input.size() < 3) {
                return null;
            }
            return new Schema(input.getField(2));
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String getInitial() {
        return Initial.class.getName();
    }

    @Override
    public String getIntermed() {
        return Intermed.class.getName();
    }

    @Override
    public String getFinal() {
        return Final.class.getName();
    }

    /*
     * Same as normal code-path exec, but outputs a Tuple with the schema
     * <Int, Int, DataBag> -- same schema as expected input.
     */
    static public class Initial extends EvalFunc<Tuple> {
        //private static final Log log = LogFactory.getLog(Initial.class);
        //private final Random randomizer = new Random();
        @Override
        public Tuple exec(Tuple tuple) throws IOException {
            if (tuple == null || tuple.size() < 3) {
                return null;
            }
            
            try {
                int n = (Integer) tuple.get(0);
                int fieldNum = (Integer) tuple.get(1);
                DataBag inputBag = (DataBag) tuple.get(2);
                if (inputBag == null) {
                    return null;
                }
                Tuple retTuple = mTupleFactory.newTuple(3);
                DataBag outputBag = mBagFactory.newDefaultBag();
                // initially, there should only be one, so not much point in doing the priority queue
                for (Tuple t : inputBag) {
                    outputBag.add(t);
                }
                retTuple.set(0, n);
                retTuple.set(1,fieldNum);
                retTuple.set(2, outputBag);               
                return retTuple;
            } catch (Exception e) {
                throw new RuntimeException("General Exception executing function: ", e);
            }
        }
    }

    static public class Intermed extends EvalFunc<Tuple> {
        private static final Log log = LogFactory.getLog(Intermed.class);
        private final Random randomizer = new Random();
        /* The input is a tuple that contains a single bag.
         * This bag contains outputs of the Initial step --
         * tuples of the format (limit, index, { top_tuples })
         * 
         * We need to take the top of tops and return a similar tuple.
         * 
         * (non-Javadoc)
         * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
         */
        @Override
        public Tuple exec(Tuple input) throws IOException {
            if (input == null || input.size() < 1) {
                return null;
            }
            try {
                DataBag bagOfIntermediates = (DataBag) input.get(0);
                Iterator<Tuple> intermediateIterator = bagOfIntermediates.iterator();
                if (!intermediateIterator.hasNext()) {
                    return null;
                }
                Tuple peekTuple = intermediateIterator.next();
                if (peekTuple == null || peekTuple.size() < 3 ) return null;
                int n = (Integer) peekTuple.get(0);
                int fieldNum = (Integer) peekTuple.get(1);
                DataBag inputBag = (DataBag) peekTuple.get(2);
                boolean allInputBagsNull = true;

                PriorityQueue<Tuple> store = new PriorityQueue<Tuple>(n + 1,
                        new TupleComparator(fieldNum));

                if (inputBag != null) {
                    allInputBagsNull = false;
                    updateTop(store, n, inputBag);
                }

                while (intermediateIterator.hasNext()) {
                    Tuple t = intermediateIterator.next();
                    if (t == null || t.size() < 3 ) continue;
                    inputBag = (DataBag) t.get(2);
                    if (inputBag != null) {
                        allInputBagsNull = false;
                        updateTop(store, n, inputBag);
                    }
                }   

                Tuple retTuple = mTupleFactory.newTuple(3);
                retTuple.set(0, n);
                retTuple.set(1,fieldNum);
                DataBag outputBag = null;
                if (!allInputBagsNull) {
                    outputBag = mBagFactory.newDefaultBag();
                    for (Tuple t : store) {
                        outputBag.add(t);
                    }
                }
                retTuple.set(2, outputBag);
                if (log.isDebugEnabled()) { 
                    if (randomizer.nextInt(1000) == 1) log.debug("outputting "+retTuple.toDelimitedString("\t")); 
                }
                return retTuple;
            } catch (ExecException e) {
                throw new RuntimeException("ExecException executing function: ", e);
            } catch (Exception e) {
                throw new RuntimeException("General Exception executing function: ", e);
            }
        }
        
    }
    
    static public class Final extends EvalFunc<DataBag> {

        private static final Log log = LogFactory.getLog(Final.class);
        private final Random randomizer = new Random();



        /*
         * The input to this function is a tuple that contains a single bag.
         * This bag, in turn, contains outputs of the Intermediate step -- 
         * tuples of the format (limit, index, { top_tuples } )
         * 
         * we want to return a bag of top tuples
         * 
         * (non-Javadoc)
         * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
         */
        @Override
        public DataBag exec(Tuple tuple) throws IOException {
            if (tuple == null || tuple.size() < 1) {
                return null;
            }
            try {
                DataBag bagOfIntermediates = (DataBag) tuple.get(0);
                Iterator<Tuple> intermediateIterator = bagOfIntermediates.iterator();
                if (!intermediateIterator.hasNext()) {
                    return null;
                }
                Tuple peekTuple = intermediateIterator.next();
                if (peekTuple == null || peekTuple.size() < 3 ) return null;
                int n = (Integer) peekTuple.get(0);
                int fieldNum = (Integer) peekTuple.get(1);
                DataBag inputBag = (DataBag) peekTuple.get(2);
                boolean allInputBagsNull = true;

                PriorityQueue<Tuple> store = new PriorityQueue<Tuple>(n + 1,
                        new TupleComparator(fieldNum));

                if (inputBag != null) {
                    allInputBagsNull = false;
                    updateTop(store, n, inputBag);
                }

                while (intermediateIterator.hasNext()) {
                    Tuple t = intermediateIterator.next();
                    if (t == null || t.size() < 3 ) continue;
                    inputBag = (DataBag) t.get(2);
                    if (inputBag != null) {
                        allInputBagsNull = false;
                        updateTop(store, n, inputBag);
                    }
                }   

                if (allInputBagsNull) {
                    return null;
                }
                
                DataBag outputBag = mBagFactory.newDefaultBag();
                for (Tuple t : store) {
                    outputBag.add(t);
                }
                if (log.isDebugEnabled()) {
                    if (randomizer.nextInt(1000) == 1) for (Tuple t : outputBag) log.debug("outputting "+t.toDelimitedString("\t"));
                }
                return outputBag;
            } catch (ExecException e) {
                throw new RuntimeException("ExecException executing function: ", e);
            } catch (Exception e) {
                throw new RuntimeException("General Exception executing function: ", e);
            }
        }
    }
}

