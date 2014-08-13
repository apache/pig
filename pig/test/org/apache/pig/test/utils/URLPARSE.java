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

package org.apache.pig.test.utils;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

public class URLPARSE extends EvalFunc<Map<String, Object> > {

    public Map<String, Object> exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        String url;

        try{
            url = (String)input.get(0);
        } catch(Exception e){
            System.out.println("Can't convert field to a string; error = " + e.getMessage());
            return null;
        }

        HashMap<String, Object> output = new HashMap<String, Object>();
        if(url == null) {
            return null;
        }

        String[] s = url.split(",");
        output.put("url",       s[0]);
        output.put("name",      s[1]);

        return output;
    }
    
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName("urlparse", input), DataType.MAP));
    }
}

