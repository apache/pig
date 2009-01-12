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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.DataType;

public class operatorHelper {
    public static int numTypes(){
        byte[] types = genAllTypes();
        return types.length;
    }
    public static byte[] genAllTypes(){
        byte[] types = { DataType.BAG, DataType.BOOLEAN, DataType.BYTEARRAY, DataType.CHARARRAY, 
                DataType.DOUBLE, DataType.FLOAT, DataType.INTEGER, DataType.LONG, DataType.MAP, DataType.TUPLE};
        return types;
    }
    
    private static String[] genAllTypeNames(){
        String[] names = { "BAG", "BOOLEAN", "BYTEARRAY", "CHARARRAY", "DOUBLE", "FLOAT", "INTEGER", "LONG", 
                "MAP", "TUPLE" };
        return names;
    }
    
    public static Map<Byte, String> genTypeToNameMap(){
        byte[] types = genAllTypes();
        String[] names = genAllTypeNames();
        Map<Byte,String> ret = new HashMap<Byte, String>();
        for(int i=0;i<types.length;i++){
            ret.put(types[i], names[i]);
        }
        return ret;
    }
}
