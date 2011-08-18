/*
 * Licensed to the Apache Software Foundation (ASF) under one or more                  
 * contributor license agreements.  See the NOTICE file distributed with               
 * this work for additional information regarding copyright ownership.                 
 * The ASF licenses this file to You under the Apache License, Version 2.0             
 * (the "License"); you may not use this file except in compliance with                
 * the License.  You may obtain a copy of the License at                               
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
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import java.util.*;

public class TOBAG extends EvalFunc<DataBag> {
	
    //String List<String> bagColumnNames = new ArrayList<String>();
    //byte List<byte> fieldTypes = new ArrayList<byte>();

    String bagColName;
    String tupleColName;
    byte   fieldType;

    public TOBAG() {}
    

    public TOBAG(String bagColName, String tupleColName, String fieldType) {

        if ( bagColName== null || tupleColName == null  || fieldType == null)  {
	    throw new RuntimeException("The bagColName  and fieldType cannot be null");
        }

    	this.bagColName   = bagColName;	
    	this.tupleColName = tupleColName;	

        if ( fieldType.equalsIgnoreCase( "CHARARRAY" )){ 
             this.fieldType = DataType.CHARARRAY;

        } else if ( fieldType.equalsIgnoreCase( "DOUBLE" )){ 
            this.fieldType = DataType.DOUBLE;

        } else if ( fieldType.equalsIgnoreCase( "FLOAT" )){ 
            this.fieldType = DataType.FLOAT; 

        } else if ( fieldType.equalsIgnoreCase( "BOOLEAN" )){ 
            this.fieldType = DataType.BOOLEAN; 

        } else if ( fieldType.equalsIgnoreCase( "INTEGER" )){ 
            this.fieldType = DataType.INTEGER;

        } else if ( fieldType.equalsIgnoreCase( "LONG" )){ 
            this.fieldType = DataType.LONG; 

        } else if ( fieldType.equalsIgnoreCase( "MAP" )){ 
            this.fieldType = DataType.MAP; 
        } else {
	    throw new RuntimeException("This type"+ fieldType +"is not supported in TOBAG");
        }

    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {

            DataBag bag =  DefaultBagFactory.getInstance().newDefaultBag();
       
            if  ( input == null  || input.size()== 0) {
                return bag; //an empty bag
            }
            if ( this.fieldType == DataType.MAP ) { 

              Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
              t.set(0, createMap(input));

              bag.add( t ); 

            } else {
               bag.add(input);
            }

            return bag;

        } catch (Exception e) {
	   throw new RuntimeException( "Error while computing size in " + this.getClass().getSimpleName());

        }
    }


    private HashMap<String, Object> createMap(Tuple input ) throws IOException {

        try {

            HashMap<String, Object> map = new HashMap<String, Object>();

            if  ( input == null  || input.size()== 0) {
                return map; //an empty map
            }

            for (int i = 0; i < input.size(); i=i+2) {

               String key= input.get(i).toString();
               if ( null!=key  && ( i+1 < input.size() )) {

                  map.put( key , input.get(i+1));

               }
            }

            return map;

        } catch (Exception e) {

            int errCode = 2106;
            String msg = "Error while creating map with" + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }

    }

    @Override
    public Schema outputSchema(Schema input) {

        try {

	     // subschema describing the fields in the tuples of the bag
            List<Schema.FieldSchema> tokenFs = new ArrayList<Schema.FieldSchema>();
   	    //tokenFs.add(new Schema.FieldSchema(null, this.fieldType )); 
   	    tokenFs.add(new Schema.FieldSchema( this.tupleColName.toUpperCase(), this.fieldType )); 

            Schema tupleSchema = new Schema( tokenFs );
            Schema.FieldSchema tupleFs = new Schema.FieldSchema( this.bagColName.toUpperCase(), tupleSchema, DataType.TUPLE);

            Schema bagSchema = new Schema(tupleFs);
            bagSchema.setTwoLevelAccessRequired(true);
            Schema.FieldSchema bagFs = new Schema.FieldSchema( null, bagSchema, DataType.BAG);

            return new Schema(bagFs);

        } catch (FrontendException e) {
            throw new RuntimeException("Unable to create schema for BAG.");
        }
 
    }
}
