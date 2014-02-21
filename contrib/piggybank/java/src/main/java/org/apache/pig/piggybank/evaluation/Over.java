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
package org.apache.pig.piggybank.evaluation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.AVG;
import org.apache.pig.builtin.COUNT;
import org.apache.pig.builtin.DoubleAvg;
import org.apache.pig.builtin.DoubleMax;
import org.apache.pig.builtin.DoubleMin;
import org.apache.pig.builtin.DoubleSum;
import org.apache.pig.builtin.FloatAvg;
import org.apache.pig.builtin.FloatMax;
import org.apache.pig.builtin.FloatMin;
import org.apache.pig.builtin.IntAvg;
import org.apache.pig.builtin.IntMax;
import org.apache.pig.builtin.IntMin;
import org.apache.pig.builtin.LongAvg;
import org.apache.pig.builtin.LongMax;
import org.apache.pig.builtin.LongMin;
import org.apache.pig.builtin.LongSum;
import org.apache.pig.builtin.MAX;
import org.apache.pig.builtin.MIN;
import org.apache.pig.builtin.StringMax;
import org.apache.pig.builtin.StringMin;
import org.apache.pig.builtin.SUM;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Given an aggregate function, a bag, and possibly a window definition,
 * produce output that matches SQL OVER.  It is the reponsibility of the caller
 * to have already ordered the bag as required by their operation.
 * The aggregate, and window definition are passed in the constructor.  The bag
 * is passed to exec each time.
 *
 * <p>Usage: Over(bag, function_to_call[, window_start, window_end[, function specific args]])
 * 
 * <p>bag - The bag to be called.  Most functions assume this is a bag with tuples
 * of a single field.
 * <p>function_to_call - Can be one of the following: <ul>
 *           <li>count</li>
 *           <li>sum(double)
 *           <li>sum(float)</li>
 *           <li>sum(int)</li>
 *           <li>sum(long)</li>
 *           <li>sum(bytearray)</li>
 *           <li>avg(double)</li>
 *           <li>avg(float)</li>
 *           <li>avg(long)</li>
 *           <li>avg(int)</li>
 *           <li>avg(bytearray)</li>
 *           <li>min(double)</li>
 *           <li>min(float)</li>
 *           <li>min(long)</li>
 *           <li>min(int)</li>
 *           <li>min(chararray)</li>
 *           <li>min(bytearray)</li>
 *           <li>max(double)</li>
 *           <li>max(float)</li>
 *           <li>max(long)</li>
 *           <li>max(int)</li>
 *           <li>max(chararray)</li>
 *           <li>max(bytearray)</li>
 *           <li>row_number</li>
 *           <li>first_value</li>
 *           <li>last_value</li>
 *           <li>lead</li>
 *           <li>lag</li>
 *           <li>rank</li>
 *           <li>dense_rank</li>
 *           <li>ntile</li>
 *           <li>percent_rank</li>
 *           <li>cume_dist</li>
 * </ul>
 * <p>window_start - optional - Record to start window on for the function.  -1
 * indicates 'unbounded preceding', i.e. the beginning of the bag.  A positive 
 * integer indicates that number of records before the current record.  0 
 * indicates the current record.  If not specified -1 is the default.
 * <p>window_end - optional - Record to end window on for the function.  -1
 * indicates 'unbounded following', i.e. the end of the bag.  A positive
 * integer indicates that number of records after the current record.  0
 * indicates teh current record.  If not specified 0 is the default.
 * <p>function_specific_args - maybe optional - The following functions accept 
 * require additional arguments: <ul>
 *           <li>lead - two optional arguments, first number of records ahead
 *           of current to lead, second default value when lead extends beyond
 *           the end of the window frame.</li>
 *           <li>lag - two optional arguments, first number of records behind
 *           of current to lag, second default value when lag extends beyond
 *           the beginning of the window frame.</li>
 *           <li>rank - one required, the number of the field the bag is
 *           ordered by</li>
 *           <li>dense_rank - one required, the number of the field the bag is
 *           ordered by</li>
 *           <li>ntile - one required, the number of buckets to split the data
 *           into</li>
 *           <li>percent_rank - one required, the number of the field the bag
 *           is ordered by</li>
 *           <li>cume_dist - one required, the number of the field the bag is
 *           ordered by</li>
 * </ul>
 *
 * <p>Example Usage:
 * <p>To do a cumulative sum:
 * <p><pre> A = load 'T';
 * B = group A by si
 * C = foreach B {
 *     C1 = order A by d;
 *     generate flatten(Stitch(C1, Over(C1.f, 'sum(float)')));
 * }
 * D = foreach C generate s, $9;</pre>
 * <p> This is equivalent to the SQL statement
 * <p><tt>select s, sum(f) over (partition by si order by d) from T;</tt>
 *
 * <p>To find the record 3 ahead of the current record, using a window between
 * the current row and 3 records ahead and a default value of 0.
 * <p><pre> A = load 'T';
 * B = group A by si;
 * C = foreach B {
 *     C1 = order A by i;
 *     generate flatten(Stitch(C1, Over(C1.i, 'lead', 0, 3, 3, 0)));
 * }
 * D = foreach C generate s, $9;</pre>
 * <p> This is equivalent to the SQL statement
 * <p><tt>select s, lead(i, 3, 0) over (partition by si order by i rows between
         * current row and 3 following) over T;</tt>
 *
 */
public class Over extends EvalFunc<DataBag> {

    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    private int rowsBefore;
    private int rowsAfter;
    private String agg;
    private boolean initialized;
    private EvalFunc<? extends Object> func;
    private Object[] udfArgs;
    private byte returnType;

    public Over() {
        initialized = false;
        udfArgs = null;
        func = null;
        returnType = DataType.UNKNOWN;
    }

    public Over(String returnType) {
        this();
        this.returnType = DataType.findTypeByName(returnType);
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
            int errCode = 2107; // TODO not sure this is the right one
            String msg = "Over expected 2 or more inputs but received "
                + input.size();
            throw new ExecException(msg, errCode, PigException.INPUT);
        }

        DataBag inbag = null;
        try {
            inbag = (DataBag)input.get(0);
        } catch (ClassCastException cce) {
            int errCode = 2107; // TODO not sure this is the right one
            String msg = "Over expected a bag for arg 1 but received " +
                DataType.findTypeName(input.get(0));
            throw new ExecException(msg, errCode, PigException.INPUT);
        }

        if (!initialized) {
            init(input);
        } else {
            if (func instanceof ResetableEvalFunc) {
                ((ResetableEvalFunc)func).reset();
            }
        }

        // Copy the bag into a special bag that we can offset into so we don't
        // have to copy once for each row
        OverBag tmpbag = new OverBag(inbag, rowsBefore, rowsAfter);
        Tuple tmptuple = mTupleFactory.newTuple(1);
        tmptuple.set(0, tmpbag);
        DataBag outbag = BagFactory.getInstance().newDefaultBag();

        for (int i = 0; i < inbag.size(); i++) {
            tmpbag.setCurrentRow(i);
            Tuple t = mTupleFactory.newTuple(1);
            t.set(0, func.exec(tmptuple));
            outbag.add(t);
        }

        return outbag;
    }

    @Override
    public Schema outputSchema(Schema inputSch) {
        try {
            if (returnType == DataType.UNKNOWN) {
                return Schema.generateNestedSchema(DataType.BAG, DataType.NULL);
            } else {
                return Schema.generateNestedSchema(DataType.BAG, returnType);
            }
        } catch (FrontendException fe) {
            throw new RuntimeException("Unable to create nested schema", fe);
        }
    }

    private void init(Tuple input) throws IOException {
        initialized = true;

        // Look for the aggregate in arg 2
        try {
            agg = (String)input.get(1);
        } catch (ClassCastException cce) {
            int errCode = 2107; // TODO not sure this is the right one
            String msg = "Over expected a string for arg 2 but received " +
                DataType.findTypeName(input.get(1));
            throw new ExecException(msg, errCode, PigException.INPUT);
        }

        // See if there is a preceding value specified, if not, unbounded
        // preceding is the default
        rowsBefore = -1;
        if (input.size() > 2) {
            try {
                rowsBefore = (Integer)input.get(2);
            } catch (ClassCastException cce) {
                int errCode = 2107; // TODO not sure this is the right one
                String msg = "Over expected an integer for arg 3 but " +
                    "received " + DataType.findTypeName(input.get(2));
                throw new ExecException(msg, errCode, PigException.INPUT);
            }
        }

        // See if there is a preceding value specified, if not, current row
        // is the default
        rowsAfter = 0;
        if (input.size() > 3) {
            try {
                rowsAfter = (Integer)input.get(3);
            } catch (ClassCastException cce) {
                int errCode = 2107; // TODO not sure this is the right one
                String msg = "Over expected an integer for arg 4 but " +
                    "received " + DataType.findTypeName(input.get(3));
                throw new ExecException(msg, errCode, PigException.INPUT);
            }
        }

        // Place any additional arguments in the udfArgs array to be passed
        // to the UDF each time
        if (input.size() > 4) {
            udfArgs = new Object[input.size() - 4];
            for (int i = 0; i < input.size() - 4; i++) {
                udfArgs[i] = input.get(i + 4);
            }
        }

        if ("count".equalsIgnoreCase(agg)) {
            func = new COUNT();
        } else if ("sum(double)".equalsIgnoreCase(agg) || 
            "sum(float)".equalsIgnoreCase(agg)) {
            func = new DoubleSum();
        } else if ("sum(int)".equalsIgnoreCase(agg) || 
            "sum(long)".equalsIgnoreCase(agg)) {
            func = new LongSum();
        } else if ("sum(bytearray)".equalsIgnoreCase(agg)) {
            func = new SUM();
        } else if ("avg(double)".equalsIgnoreCase(agg)) {
            func = new DoubleAvg();
        } else if ("avg(float)".equalsIgnoreCase(agg)) {
            func = new FloatAvg();
        } else if ("avg(long)".equalsIgnoreCase(agg)) {
            func = new LongAvg();
        } else if ("avg(int)".equalsIgnoreCase(agg)) {
            func = new IntAvg();
        } else if ("avg(bytearray)".equalsIgnoreCase(agg)) {
            func = new AVG();
        } else if ("min(double)".equalsIgnoreCase(agg)) {
            func = new DoubleMin();
        } else if ("min(float)".equalsIgnoreCase(agg)) {
            func = new FloatMin();
        } else if ("min(long)".equalsIgnoreCase(agg)) {
            func = new LongMin();
        } else if ("min(int)".equalsIgnoreCase(agg)) {
            func = new IntMin();
        } else if ("min(chararray)".equalsIgnoreCase(agg)) {
            func = new StringMin();
        } else if ("min(bytearray)".equalsIgnoreCase(agg)) {
            func = new MIN();
        } else if ("max(double)".equalsIgnoreCase(agg)) {
            func = new DoubleMax();
        } else if ("max(float)".equalsIgnoreCase(agg)) {
            func = new FloatMax();
        } else if ("max(long)".equalsIgnoreCase(agg)) {
            func = new LongMax();
        } else if ("max(int)".equalsIgnoreCase(agg)) {
            func = new IntMax();
        } else if ("max(chararray)".equalsIgnoreCase(agg)) {
            func = new StringMax();
        } else if ("max(bytearray)".equalsIgnoreCase(agg)) {
            func = new MAX();
        } else if ("row_number".equalsIgnoreCase(agg)) {
            func = new RowNumber();
        } else if ("first_value".equalsIgnoreCase(agg)) {
            func = new FirstValue();
        } else if ("last_value".equalsIgnoreCase(agg)) {
            func = new LastValue();
        } else if ("lead".equalsIgnoreCase(agg)) {
            func = new Lead(udfArgs);
        } else if ("lag".equalsIgnoreCase(agg)) {
            func = new Lag(udfArgs);
        } else if ("rank".equalsIgnoreCase(agg)) {
            func = new Rank(udfArgs);
        } else if ("dense_rank".equalsIgnoreCase(agg)) {
            func = new DenseRank(udfArgs);
        } else if ("ntile".equalsIgnoreCase(agg)) {
            func = new Ntile(udfArgs);
        } else if ("percent_rank".equalsIgnoreCase(agg)) {
            func = new PercentRank(udfArgs);
        } else if ("cume_dist".equalsIgnoreCase(agg)) {
            //func = new CumeDist(udfArgs);
            func = new CumeDist();
        } else if ("debug".equalsIgnoreCase(agg)) {
            func = new Debug();
        } else {
            throw new ExecException("Unknown aggregate " + agg);
        }
    }

    static class OverBag implements DataBag {

        private List<Tuple> tuples;
        private int before;
        private int after;
        private int currentRow;

        OverBag(DataBag bag, int before, int after) {
            addAll(bag);
            this.before = before;
            this.after = after;
            currentRow = 0;
        }

        public long size() {
            return endPosition() - startPosition();
        }

        public boolean isSorted() {
            return false; // don't actually know
        }
    
        public boolean isDistinct() {
            return false;
        }
    
        public Iterator<Tuple> iterator() {
            return new OverBagIterator(tuples, currentRow, startPosition(),
                    endPosition());
        }

        public void add(Tuple t) {
            throw new RuntimeException("OverBag.add not implemented");
        }

        public void addAll(DataBag b) {
            tuples = new ArrayList<Tuple>((int)b.size());
            for (Tuple t : b) {
                tuples.add(t);
            }
        }

        public void clear() {
            throw new RuntimeException("OverBag.clear not implemented");
        }

        public void markStale(boolean stale) {
            throw new RuntimeException("OverBag.markStale not implemented");
        }

        public void readFields(java.io.DataInput in) {
            throw new RuntimeException("OverBag.readFields not implemented");
        }

        public void write(java.io.DataOutput out) {
            throw new RuntimeException("OverBag.write not implemented");
        }

        public int compareTo(Object o) {
            throw new RuntimeException("OverBag.compareTo not implemented");
        }

        void setCurrentRow(int cr) {
            currentRow = cr;
        }

        private int startPosition() {
            return (before == -1 ? 0 : currentRow - before);
        }

        private int endPosition() {
            return (after == -1 ? tuples.size() : currentRow + after + 1);
        }

        static class OverBagIterator implements Iterator<Tuple> {

            List<Tuple> tuples;
            int currentRow; // from the UDFs perspective
            int begin;
            int end;
            int nextRow; // next row this iterator will return

            OverBagIterator(List<Tuple> tuples,
                            int currentRow,
                            int begin,
                            int end) {
                this.tuples = tuples;
                this.currentRow = currentRow;
                this.begin = begin;
                this.end = end;
                nextRow = begin;
            }

            public boolean hasNext() {
                return nextRow < end;
            }

            public Tuple next() {
                try {
                    // Check if the beginning of frame is positioned before the
                    // beginning of the bag.
                    if (nextRow < 0) return mTupleFactory.newTuple(1);

                    // Check if the pointer has moved past the end of the window 
                    if (nextRow >= tuples.size()) {
                        return mTupleFactory.newTuple(1);
                    }

                    return tuples.get(nextRow);
                } finally {
                    // Placed here so we increment it no matter what.
                    nextRow++;
                }
            }

            public void remove() {
                throw new RuntimeException(
                        "OverBagIterator.remove not implemented");
            }
        }

        public long spill() {
            return 0;
        }
    
        public long getMemorySize() {
            return 0;
        }

    }

    private static abstract class ResetableEvalFunc<K> extends EvalFunc<K> {

        protected int currentRow;

        ResetableEvalFunc() {
            reset();
        }

         void reset() {
             currentRow = 0;
         }
    }

    // Makes some serious assumptions about how many times it's called, don't call
    // it any extra times.
    private static class RowNumber extends ResetableEvalFunc<Integer> {

        @Override
        public Integer exec(Tuple input) throws IOException {
            return ++currentRow;
        }
    }

    private static class FirstValue extends EvalFunc<Object> {
        @Override
        public Object exec(Tuple input) throws IOException {
            DataBag inbag = (DataBag)input.get(0);
            if (inbag.size() == 0) return null;
            return inbag.iterator().next().get(0);
        }
    }
    
    private static class LastValue extends EvalFunc<Object> {
        @Override
        public Object exec(Tuple input) throws IOException {
            DataBag inbag = (DataBag)input.get(0);
            OverBag.OverBagIterator iter =
                (OverBag.OverBagIterator)inbag.iterator();
            return iter.tuples.get(iter.end - 1).get(0);
        }
    }

    // Makes some serious assumptions about how many times it's called, don't call
    // it any extra times.
    private static class Lead extends ResetableEvalFunc<Object> {
        int rowsAhead;
        Object deflt;

        Lead(Object[] args) throws IOException {
            rowsAhead = 1;
            deflt = null;
            if (args != null) {
                if (args.length >= 1) {
                    try {
                        rowsAhead = (Integer)args[0];
                    } catch (ClassCastException cce) {
                        int errCode = 2107; // TODO not sure this is the right one
                        String msg = "Lead expected an integer for arg 2 " +
                            " but received " + DataType.findTypeName(args[0]);
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                }
                if (args.length >= 2) {
                    deflt = args[1];
                }
            }
            reset();
        }

        @Override
        public Object exec(Tuple input) throws IOException {
            DataBag inbag = (DataBag)input.get(0);
            OverBag.OverBagIterator iter =
                (OverBag.OverBagIterator)inbag.iterator();
            if (currentRow < iter.tuples.size()) {
                return iter.tuples.get(currentRow++).get(0);
            } else if (deflt != null) {
                return deflt;
            } else {
                return null;
            }
        }

        @Override
        void reset() {
            currentRow = rowsAhead;
        }
    }

    // Makes some serious assumptions about how many times it's called, don't call
    // it any extra times.
    private static class Lag extends ResetableEvalFunc<Object> {
        int rowsBehind;
        Object deflt;

        Lag(Object[] args) throws IOException {
            rowsBehind = 1;
            deflt = null;
            if (args != null) {
                if (args.length >= 1) {
                    try {
                        rowsBehind = (Integer)args[0];
                    } catch (ClassCastException cce) {
                        int errCode = 2107; // TODO not sure this is the right one
                        String msg = "Lag expected an integer for arg 2 " +
                            " but received " + DataType.findTypeName(args[0]);
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                }
                if (args.length >= 2) {
                    deflt = args[1];
                }
            }
            reset();
        }

        @Override
        public Object exec(Tuple input) throws IOException {
            DataBag inbag = (DataBag)input.get(0);
            OverBag.OverBagIterator iter =
                (OverBag.OverBagIterator)inbag.iterator();
            try {
                if (currentRow >= 0) {
                    return iter.tuples.get(currentRow).get(0);
                } else if (deflt != null) {
                    return deflt;
                } else {
                    return null;
                }
            } finally {
                currentRow++;
            }
        }

        @Override
        void reset() {
            currentRow = -1 * rowsBehind;
        }
    }
    
    // Makes some serious assumptions about how many times it's called, don't
    // call it any extra times.
    private static abstract class BaseRank<T> extends ResetableEvalFunc<T> {
        Object[] lastKey;
        int[] orderFields;
        int lastRankUsed;
        int timesThisRankUsed;

        protected BaseRank(Object[] args) throws IOException {

            if (args == null || args.length < 1) {
                throw new ExecException(
                    "Rank args must contain ordering column numbers, "
                    + "e.g. rank(1, 2)", 2107, PigException.INPUT);
            }

            lastKey = new Object[args.length];
            orderFields = new int[args.length];
            for (int i = 0; i < args.length; i++) {
                try {
                    orderFields[i] = (Integer)args[i];
                } catch (ClassCastException cce) {
                    throw new ExecException(
                        "Rank expected column number in arg " + i +
                        " but received " + DataType.findTypeName(args[i]),
                        2107, PigException.INPUT);
                }
            }

            reset();
        }

        @Override
        public T exec(Tuple input) throws IOException {
            DataBag inbag = (DataBag)input.get(0);
            OverBag.OverBagIterator iter =
                (OverBag.OverBagIterator)inbag.iterator();

            if (lastRankUsed == 0) {
                // First record
                for (int i = 0; i < lastKey.length; i++) {
                    lastKey[i] = iter.tuples.get(0).get(orderFields[i]);
                }
                lastRankUsed = 1;
            } else {
                // Check to see if the keys have changed
                boolean keyChange = false;
                for (int i = 0; i < lastKey.length && !keyChange; i++) {
                    Object currentKey =
                        iter.tuples.get(currentRow).get(orderFields[i]);
                    if (lastKey[i] == null) {
                        keyChange |= currentKey != null;
                    } else {
                        keyChange |= !lastKey[i].equals(currentKey);
                    }
                }
                if (keyChange) {
                    incrementRank();
                    timesThisRankUsed = 1;
                    for (int i = 0; i < lastKey.length; i++) {
                        lastKey[i] =
                            iter.tuples.get(currentRow).get(orderFields[i]);
                    }
                } else {
                    timesThisRankUsed++;
                }
            }

            currentRow++;
            return calculateRank(iter);
        }

        @Override
        void reset() {
            super.reset();
            lastRankUsed = 0;
            timesThisRankUsed = 1;
        }

        abstract protected void incrementRank();

        abstract protected T calculateRank(OverBag.OverBagIterator iter);
    }

    private static class Rank extends BaseRank<Integer> {
        Rank(Object[] args) throws IOException {
            super(args);
        }

        protected void incrementRank() {
            lastRankUsed += timesThisRankUsed;
        }

        protected Integer calculateRank(OverBag.OverBagIterator iter) {
            return lastRankUsed;
        }
    }

    private static class DenseRank extends BaseRank<Integer> {
        DenseRank(Object[] args) throws IOException {
            super(args);
        }

        protected void incrementRank() {
            lastRankUsed++;
        }

        protected Integer calculateRank(OverBag.OverBagIterator iter) {
            return lastRankUsed;
        }
    }

    private static class PercentRank extends BaseRank<Double> {
        PercentRank(Object[] args) throws IOException {
            super(args);
        }

        protected void incrementRank() {
            lastRankUsed += timesThisRankUsed;
        }

        protected Double calculateRank(OverBag.OverBagIterator iter) {
            return ((double)lastRankUsed - 1.0 ) /
                ((double)iter.tuples.size() - 1.0);
        }
    }

    /*
    private static class CumeDist extends BaseRank<Double> {
        CumeDist(Object[] args) throws IOException {
            super(args);
        }

        protected void incrementRank() {
            lastRankUsed += timesThisRankUsed;
        }

        protected Double calculateRank(OverBag.OverBagIterator iter) {
            return ((double)lastRankUsed) / (double)iter.tuples.size();
        }
    }
    */

    private static class CumeDist extends ResetableEvalFunc<Double> {

        @Override
        public Double exec(Tuple input) throws IOException {
            DataBag inbag = (DataBag)input.get(0);
            OverBag.OverBagIterator iter =
                (OverBag.OverBagIterator)inbag.iterator();

            return ((double)++currentRow)/(double)iter.tuples.size();
        }
    }




    // Makes some serious assumptions about how many times it's called, don't
    // call it any extra times.
    private static class Ntile extends ResetableEvalFunc<Integer> {
        int numBuckets;

        protected Ntile(Object[] args) throws IOException {

            if (args == null || args.length != 1) {
                throw new ExecException(
                    "Ntile args must contain arg describing how to split data, "
                    + "e.g. ntile(4)", 2107, PigException.INPUT);
            }

            try {
                numBuckets = (Integer)args[0];
            } catch (ClassCastException cce) {
                throw new ExecException(
                    "Ntile expected integer argument but received " +
                    DataType.findTypeName(args[0]), 2107, PigException.INPUT);
            }
            reset();

        }

        @Override
        public Integer exec(Tuple input) throws IOException {
            DataBag inbag = (DataBag)input.get(0);
            OverBag.OverBagIterator iter =
                (OverBag.OverBagIterator)inbag.iterator();

            int val = 0;
            if (numBuckets >= iter.tuples.size()) val = currentRow + 1;
            else val = currentRow * numBuckets / iter.tuples.size() + 1;

            currentRow++;
            return val;
        }

    }

    private static class Debug extends EvalFunc<String> {

        @Override
        public String exec(Tuple input) throws IOException {
            DataBag inbag = (DataBag)input.get(0);
            OverBag.OverBagIterator iter =
                (OverBag.OverBagIterator)inbag.iterator();
            System.out.println("Current row " + iter.currentRow + " begin "
                    + iter.begin + " end " + iter.end + " nextRow " +
                    iter.nextRow + " size " + iter.tuples.size());
            System.out.print("{");
            while (iter.hasNext()) { 
                Tuple t = iter.next();
                if (t == null) System.out.print("null,");
                else System.out.print(t.toString() + ",");
            }
            System.out.println("}");
            return "bla";
        }
    }
}

