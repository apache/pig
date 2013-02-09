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
package org.apache.pig.piggybank.evaluation.datetime;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
* <dl>
* <dt><b>Syntax:</b></dt>
* <dd><code>int DiffDate(String yyyymmdd1, String yyyymmdd2)</code>.</dd>
* <dt><b>Input:</b></dt>
* <dd><code>date string in "yyyymmdd" format</code>.</dd>
* <dt><b>Output:</b></dt>
* <dd><code>(date1-date2) in days, can be negative</code>.</dd>
* </dl>
*/

public class DiffDate extends EvalFunc<Integer> {
    static DateFormat df = new SimpleDateFormat("yyyyMMdd");
    @Override
    public Schema outputSchema(Schema input) {
        try {
            return new Schema(new Schema.FieldSchema(getSchemaName(this
                    .getClass().getName().toLowerCase(), input),
                    DataType.INTEGER));
        } catch (Exception e) {
            return null;
        }
    }
    @Override
    public Integer exec(Tuple input) throws IOException {
        if (input.size()!=2) {
            String msg = "DiffDate : Only 2 parameters are allowed.";
            throw new IOException(msg);
        }
        String strDate1 = (String)input.get(0);
        String strDate2 = (String)input.get(1);
        
        if (input.get(0)==null || input.get(1)==null)
            return null;
        
        Date date1 = null;
        Date date2 = null;
        
        try {
            date1 = df.parse(strDate1);
            date2 = df.parse(strDate2);
        } catch (ParseException e) {
            String msg = "DiffDate : Parameters have to be string in 'yyyymmdd' format.";
            warn(msg, PigWarning.UDF_WARNING_1);
            return null;
        }
        return (int)((date1.getTime() - date2.getTime())/(1000*60*60*24)); 
    }
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
}
