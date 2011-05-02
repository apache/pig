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

import org.apache.pig.EvalFunc;
import java.util.Iterator;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import java.util.HashMap;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;

public class TOMAP extends EvalFunc<HashMap<String,Object>> {

    @Override
    public HashMap<String, Object> exec(Tuple input) throws IOException {
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
        return new Schema(new Schema.FieldSchema(null, DataType.MAP));
    }
}
