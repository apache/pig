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

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

/**
 * Default implementation of format of Tuple. Dump and PigDump use this default
 * implementation
 * 
 */
public class TupleFormat {

    protected static final Log mLog = LogFactory.getLog(TupleFormat.class);

    /**
     * Default implementation of format of tuple (each filed is delimited by
     * tab)
     * 
     * @param tuple
     * @return Default format of Tuple
     */
    @SuppressWarnings("unchecked")
    public static String format(Tuple tuple) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for (int i = 0; i < tuple.size(); ++i) {
            try {
                Object d = tuple.get(i);
                if (d != null) {
                    if (d instanceof Map) {
                        sb.append(DataType.mapToString((Map<String, Object>) d));
                    } else if (d instanceof Tuple) {
                        Tuple t = (Tuple) d;
                        sb.append(TupleFormat.format(t));
                    } else if (d instanceof DataBag){
                        DataBag bag=(DataBag)d;
                        sb.append(BagFormat.format(bag));
                    }
                    else {
                        sb.append(d.toString());
                    }
                } else {
                    sb.append("");
                }
                if (i != tuple.size() - 1)
                    sb.append(",");
            } catch (ExecException e) {
                e.printStackTrace();
                mLog.warn("Exception when format tuple", e);
            }

        }
        sb.append(')');
        return sb.toString();
    }
}
