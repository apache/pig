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

package org.apache.pig.impl.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.data.DataType;

public class NumValCarrier {
    private Map<Byte,ValCarrier> byteToStr = new HashMap<Byte,ValCarrier>(10);

    public NumValCarrier() {
        ValCarrier valCarrier = new ValCarrier("val");
        ValCarrier bagCarrier = new ValCarrier("bag");
        ValCarrier tupleCarrier = new ValCarrier("tuple");
        ValCarrier mapCarrier = new ValCarrier("map");
        ValCarrier nullCarrier = new ValCarrier("null");

        byteToStr.put(DataType.BAG,bagCarrier);
        byteToStr.put(DataType.CHARARRAY,valCarrier);
        byteToStr.put(DataType.BYTEARRAY,valCarrier);
        byteToStr.put(DataType.DOUBLE,valCarrier);
        byteToStr.put(DataType.FLOAT,valCarrier);
        byteToStr.put(DataType.INTEGER,valCarrier);
        byteToStr.put(DataType.LONG,valCarrier);
        byteToStr.put(DataType.BOOLEAN,valCarrier);
        byteToStr.put(DataType.DATETIME,valCarrier);
        byteToStr.put(DataType.MAP,mapCarrier);
        byteToStr.put(DataType.TUPLE,tupleCarrier);
        byteToStr.put(DataType.NULL,nullCarrier);
        byteToStr.put(DataType.BIGINTEGER,valCarrier);
        byteToStr.put(DataType.BIGDECIMAL,valCarrier);
    }

    public String makeNameFromDataType(byte type) {
        return byteToStr.get(type).getNextString();
    }

    private static class ValCarrier {
        private int num=0;
        private String str;

        public ValCarrier(String str) {
            this.str=str;
        }

        public String getNextString() {
            return str+"_"+num++;
        }
    }
}
