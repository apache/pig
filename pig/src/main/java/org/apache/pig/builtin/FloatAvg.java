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
import java.util.Iterator;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.executionengine.ExecException;


/**
 * This method should never be used directly, use {@link AVG}.
 */
public class FloatAvg extends EvalFunc<Double> implements Algebraic, Accumulator<Double> {
    
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Double exec(Tuple input) throws IOException {
        try {
            Double sum = sum(input);
            if(sum == null) {
                // either we were handed an empty bag or a bag
                // filled with nulls - return null in this case
                return null;
            }
            double count = count(input);

            Double avg = null;
            if (count > 0)
                avg = new Double(sum / count);
    
            return avg;
        } catch (ExecException ee) {
            throw ee;
        }
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermediate.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                Tuple t = mTupleFactory.newTuple(2);
                // input is a bag with one tuple containing
                // the column we are trying to avg on
                DataBag bg = (DataBag) input.get(0);
                Float f = null;
                if(bg.iterator().hasNext()) {
                    Tuple tp = bg.iterator().next();
                    f = (Float)(tp.get(0));
                }
                t.set(0, f != null ? new Double(f) : null);
                if (f != null)
                    t.set(1, 1L);
                else
                    t.set(1, 0L);
                return t;
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing average in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);           
            }
                
        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                DataBag b = (DataBag)input.get(0);
                return combine(b);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing average in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);           
            }
        }
    }

    static public class Final extends EvalFunc<Double> {
        @Override
        public Double exec(Tuple input) throws IOException {
            try {
                DataBag b = (DataBag)input.get(0);
                Tuple combined = combine(b);

                Double sum = (Double)combined.get(0);
                if(sum == null) {
                    return null;
                }
                double count = (Long)combined.get(1);

                Double avg = null;
                if (count > 0) {
                    avg = new Double(sum / count);
                }
                return avg;
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing average in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);           
            }
        }
    }

    static protected Tuple combine(DataBag values) throws ExecException {
        double sum = 0;
        long count = 0;

        // combine is called from Intermediate and Final
        // In either case, Initial would have been called
        // before and would have sent in valid tuples
        // Hence we don't need to check if incoming bag
        // is empty

        Tuple output = mTupleFactory.newTuple(2);
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            Double d = (Double)t.get(0);
            // we count nulls in avg as contributing 0
            // a departure from SQL for performance of 
            // COUNT() which implemented by just inspecting
            // size of the bag
            if(d == null) {
                d = 0.0;
            } else {
                sawNonNull = true;
            }
            sum += d;
            count += (Long)t.get(1);
        }
        if(sawNonNull) {
            output.set(0, new Double(sum));
        } else {
            output.set(0, null);
        }
        output.set(1, Long.valueOf(count));
        return output;
    }

    static protected long count(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        Iterator it = values.iterator();
        long cnt = 0;
        while (it.hasNext()){
            Tuple t = (Tuple)it.next();
            if (t != null && t.size() > 0 && t.get(0) != null)
                cnt++;
        }

        return cnt;
    }

    static protected Double sum(Tuple input) throws ExecException, IOException {
        DataBag values = (DataBag)input.get(0);

        // if we were handed an empty bag, return NULL
        if(values.size() == 0) {
            return null;
        }

        double sum = 0.0;
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try {
                Float f = (Float)(t.get(0));
                if (f == null) continue;
                sawNonNull = true;
                sum += f;
            }catch(RuntimeException exp) {
                int errCode = 2103;
                String msg = "Problem while computing sum of floats.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }

        if(sawNonNull) {
            return new Double(sum);
        } else {
            return null;
        }
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE)); 
    }
    
    /* Accumulator interface */

    private Double intermediateSum = null;
    private Double intermediateCount = null;
    
    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            Double sum = sum(b);
            if(sum == null) {
                return;
            }
            // set default values
            if (intermediateSum == null || intermediateCount == null) {
                intermediateSum = 0.0;
                intermediateCount = 0.0;
            }
            
            double count = (Long)count(b);
            
            if (count > 0) {
                intermediateCount += count;
                intermediateSum += sum;
            }
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing average in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);           
        }
    }        

    @Override
    public void cleanup() {
        intermediateSum = null;
        intermediateCount = null;
    }

    @Override
    public Double getValue() {
        Double avg = null;
        if (intermediateCount != null && intermediateCount > 0) {
            avg = new Double(intermediateSum / intermediateCount);
        }
        return avg;
    }    

}
