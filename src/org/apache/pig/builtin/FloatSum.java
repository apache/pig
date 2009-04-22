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

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;


/**
 * Generates the sum of the Float values in the first field of a tuple.
 */
public class FloatSum extends EvalFunc<Double> implements Algebraic {

    @Override
    public Double exec(Tuple input) throws IOException {
        try {
            return sum(input);
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing sum in " + this.getClass().getSimpleName();
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
            // Initial is called in the map - for SUM
            // we just send the tuple down
            try {
                // input is a bag with one tuple containing
                // the column we are trying to sum
                DataBag bg = (DataBag) input.get(0);
                Float f = null;
                if(bg.iterator().hasNext()) {
                    Tuple tp = bg.iterator().next();
                    f = (Float)tp.get(0);
                }
                // send down a double since intermediate
                // would  be sending a double
                return tfact.newTuple(f != null ? 
                        new Double(f) : null);
            } catch (ExecException e) {
                throw e;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing sum in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        }
    }
    static public class Intermediate extends EvalFunc<Tuple> {
        private static TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                return tfact.newTuple(sumDoubles(input));
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing sum in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        }
    }
    static public class Final extends EvalFunc<Double> {
        @Override
        public Double exec(Tuple input) throws IOException {
            try {
                return sumDoubles(input);                
            } catch (ExecException ee) {
                throw ee;
            } catch (Exception e) {
                int errCode = 2106;
                String msg = "Error while computing sum in " + this.getClass().getSimpleName();
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        }
    }
    
    static protected Double sumDoubles(Tuple input) throws ExecException {
        // Can't just call sum, because the intermediate results are
        // now Doubles insteads of Floats.
        DataBag values = (DataBag)input.get(0);

        // if we were handed an empty bag, return NULL
        // this is in compliance with SQL standard
        if(values.size() == 0) {
            return null;
        }

        double sum = 0;
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = (Tuple) it.next();
            try {
                Double d = (Double)(t.get(0));
                if (d == null) continue;
                sawNonNull = true;
                sum += d;
            }catch(RuntimeException exp) {
                ExecException newE =  new ExecException("Error processing: " +
                        t.toString() + exp.getMessage(), exp);
                    throw newE;
            }
        }

        
        if(sawNonNull) {
            return new Double(sum);
        } else {
            return null;
        }

    }

    static protected  Double sum(Tuple input) throws ExecException {
        DataBag values = (DataBag)input.get(0);
        
        // if we were handed an empty bag, return NULL
        // this is in compliance with SQL standard
        if(values.size() == 0) {
            return null;
        }

        double sum = 0.0;
        boolean sawNonNull = false;
        for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
            Tuple t = (Tuple) it.next();
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

}
