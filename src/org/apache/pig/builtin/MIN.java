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
import java.util.Iterator;
import java.util.List;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;


/**
 * Generates the min of the values of the first field of a tuple.
 */
public class MIN extends EvalFunc<Double> implements Algebraic {

    @Override
    public Double exec(Tuple input) throws IOException {
        try {
            return min(input);
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing min in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);           
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
        private static TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                // input is a bag with one tuple containing
                // the column we are trying to min on
                DataBag bg = (DataBag) input.get(0);
                DataByteArray dba = null;
                if(bg.iterator().hasNext()) {
                    Tuple tp = bg.iterator().next();
                    dba = (DataByteArray)tp.get(0);
                }
                return tfact.newTuple(dba != null?
                        Double.valueOf(dba.toString()) : null);
            } catch (NumberFormatException e) {
                // invalid input, send null
                return tfact.newTuple(null);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing min in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);           
            }
        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {
        private static TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                return tfact.newTuple(minDoubles(input));
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing min in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);           
            }
        }
    }
    static public class Final extends EvalFunc<Double> {
        @Override
        public Double exec(Tuple input) throws IOException {
            try {
                return minDoubles(input);
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing min in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);           
            }
        }
    }

    static protected Double min(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        
        // if we were handed an empty bag, return NULL
        // this is in compliance with SQL standard
        if(values.size() == 0) {
            return null;
        }

        double curMin = Double.POSITIVE_INFINITY;
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try {
                DataByteArray dba = (DataByteArray)t.get(0);
                Double d = dba != null ? Double.valueOf(dba.toString()): null;
                if (d == null) continue;
                sawNonNull = true;
                curMin = java.lang.Math.min(curMin, d);
            } catch (RuntimeException exp) {
                int errCode = 2103;
                String msg = "Problem while computing min of doubles.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }
    
        if(sawNonNull) {
            return new Double(curMin);
        } else {
            return null;
        }
    }
    
    // same as above function except all its inputs are 
    // always Double - this should be used for better performance
    // since we don't have to check the type of the object to
    // decide it is a double. This should be used when the initial,
    // intermediate and final versions are used.
    static protected Double minDoubles(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        
        // if we were handed an empty bag, return NULL
        // this is in compliance with SQL standard
        if(values.size() == 0) {
            return null;
        }

        double curMin = Double.POSITIVE_INFINITY;
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = it.next();
            try {
                Double d = (Double)t.get(0);
                if (d == null) continue;
                sawNonNull = true;
                curMin = java.lang.Math.min(curMin, d);
            } catch (RuntimeException exp) {
                int errCode = 2103;
                String msg = "Problem while computing min of doubles.";
                throw new ExecException(msg, errCode, PigException.BUG, exp);
            }
        }
    
        if(sawNonNull) {
            return new Double(curMin);
        } else {
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE)); 
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), Schema.generateNestedSchema(DataType.BAG, DataType.BYTEARRAY)));
        funcList.add(new FuncSpec(DoubleMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DOUBLE)));
        funcList.add(new FuncSpec(FloatMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.FLOAT)));
        funcList.add(new FuncSpec(IntMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER)));
        funcList.add(new FuncSpec(LongMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.LONG)));
        funcList.add(new FuncSpec(StringMin.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.CHARARRAY)));
        return funcList;
    }    
}
