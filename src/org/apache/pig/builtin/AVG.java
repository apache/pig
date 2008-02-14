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
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * Generates the average of the values of the first field of a tuple. This class is Algebraic in
 * implemenation, so if possible the execution will be split into a local and global application
 */
public class AVG extends EvalFunc<DataAtom> implements Algebraic {
    
    @Override
    public void exec(Tuple input, DataAtom output) throws IOException {
        double sum = sum(input);
        double count = count(input);

        double avg = 0;
        if (count > 0)
            avg = sum / count;

        output.setValue(avg);
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermed.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }

    static public class Initial extends EvalFunc<Tuple> {
        @Override
        public void exec(Tuple input, Tuple output) throws IOException {
            try {
            output.appendField(new DataAtom(sum(input)));
            output.appendField(new DataAtom(count(input)));
            // output.appendField(new DataAtom("processed by initial"));
            } catch(RuntimeException t) {
                throw new RuntimeException(t.getMessage() + ": " + input, t);
            }
        }
    }

    static public class Intermed extends EvalFunc<Tuple> {
        @Override
        public void exec(Tuple input, Tuple output) throws IOException {
            combine(input.getBagField(0), output);
        }
    }

    static public class Final extends EvalFunc<DataAtom> {
        @Override
        public void exec(Tuple input, DataAtom output) throws IOException {
            Tuple combined = new Tuple();
            if(input.getField(0) instanceof DataBag) {
                combine(input.getBagField(0), combined);    
            } else {
                throw new RuntimeException("Bag not found in: " + input);
                
                
                //combined = input.getTupleField(0);
            }
            double sum = combined.getAtomField(0).numval();
            double count = combined.getAtomField(1).numval();

            double avg = 0;
            if (count > 0) {
                avg = sum / count;
            }
            output.setValue(avg);
        }
    }

    static protected void combine(DataBag values, Tuple output) throws IOException {
        double sum = 0;
        double count = 0;

        for (Iterator it = values.iterator(); it.hasNext();) {
            Tuple t = (Tuple) it.next();
//            if(!(t.getField(0) instanceof DataAtom)) {
//                throw new RuntimeException("Unexpected Type: " + t.getField(0).getClass().getName() + " in " + t);
//            }
            
            sum += t.getAtomField(0).numval();
            count += t.getAtomField(1).numval();
        }

        output.appendField(new DataAtom(sum));
        output.appendField(new DataAtom(count));
    }

    static protected long count(Tuple input) throws IOException {
        DataBag values = input.getBagField(0);

        
        return values.size();
    }

    static protected double sum(Tuple input) throws IOException {
        DataBag values = input.getBagField(0);

        double sum = 0;
        for (Iterator it = values.iterator(); it.hasNext();) {
            Tuple t = (Tuple) it.next();
            sum += t.getAtomField(0).numval();
        }

        return sum;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        return new AtomSchema("average");
    }

}
