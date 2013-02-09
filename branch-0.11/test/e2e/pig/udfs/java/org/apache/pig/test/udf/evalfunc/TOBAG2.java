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
package org.apache.pig.test.udf.evalfunc;


import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * This class takes a list of items and puts them into a bag
 * T = foreach U generate TOBAG($0, $1, $2);
 * It's like saying this:
 * T = foreach U generate {($0), ($1), ($2)}
 * 
 * All arguments that are not of tuple type are inserted into a tuple before
 * being added to the bag. This is because bag is always a bag of tuples.
 * 
 * Output schema:
 * The output schema for this udf depends on the schema of its arguments.
 * If all the arguments have same type and same inner 
 * schema (for bags/tuple columns), then the udf output schema would be a bag 
 * of tuples having a column of the type and inner-schema (if any) of the 
 * arguments. 
 * If the arguments are of type tuple/bag, then their innerschmea, including
 * the alias names should match.
 * If these conditions are not met the output schema will be a bag with null 
 * inner schema.
 *  
 *  example 1 
 *  grunt> describe a;
 *  a: {a0: int,a1: int}
 *  grunt> b = foreach a generate TOBAG(a0,a1);
 *  grunt> describe b;
 *  b: {{int}}
 *  
 *  example 2
 *  grunt> describe a;
 *  a: {a0: (x: int),a1: (x: int)}
 *  grunt> b = foreach a generate TOBAG(a0,a1);                                    
 *  grunt> describe b;                                                             
 *  b: {{(x: int)}}
 *  
 *  example 3
 *  grunt> describe a;                                                             
 *  a: {a0: (x: int),a1: (y: int)}
 * -- note that the inner schema is different because the alises (x & y) are different
 *  grunt> b = foreach a generate TOBAG(a0,a1);                                    
 *  grunt> describe b;                                                             
 *  b: {{NULL}}
 *  
 *  
 * 
 */
public class TOBAG2 extends EvalFunc<DataBag> {

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            DataBag bag = BagFactory.getInstance().newDefaultBag();

            for (int i = 0; i < input.size(); ++i) {
                final Object object = input.get(i);
                if (object instanceof Tuple) {
                    bag.add( (Tuple) object);
                } else {
                    Tuple tp2 = TupleFactory.getInstance().newTuple(1);
                    tp2.set(0, object);
                    bag.add(tp2);
                }
            }

            return bag;
        } catch (Exception ee) {
            throw new RuntimeException("Error while creating a bag", ee);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#outputSchema(org.apache.pig.impl.logicalLayer.schema.Schema)
     * If all the columns in the tuple are of same type, then set the bag schema 
     * to bag of tuple with column of this type
     * 
     */
    @Override
    public Schema outputSchema(Schema inputSch) {
        byte type = DataType.ERROR;
        Schema innerSchema = null;
        if(inputSch != null){
            for(FieldSchema fs : inputSch.getFields()){
                if(type == DataType.ERROR){
                    type = fs.type;
                    innerSchema = fs.schema;
                }else{
                    if( type != fs.type || !nullEquals(innerSchema, fs.schema)){
                        // invalidate the type
                        type = DataType.ERROR;
                        break;
                    }
                }
            }
        }
        try {
            if(type == DataType.ERROR){
                return Schema.generateNestedSchema(DataType.BAG, DataType.NULL);
            }
            FieldSchema innerFs = new Schema.FieldSchema(null, innerSchema, type);
            Schema innerSch = new Schema(innerFs);
            Schema bagSchema = new Schema(new FieldSchema(null, innerSch, DataType.BAG));
            return bagSchema;
        } catch (FrontendException e) {
            //This should not happen
            throw new RuntimeException("Bug : exception thrown while " +
                    "creating output schema for TOBAG udf", e);
        }

    }

    private boolean nullEquals(Schema currentSchema, Schema newSchema) {
        if(currentSchema == null){
            if(newSchema != null){
                return false;
            }
            return true;
        }
        return currentSchema.equals(newSchema);
    }
    

}

