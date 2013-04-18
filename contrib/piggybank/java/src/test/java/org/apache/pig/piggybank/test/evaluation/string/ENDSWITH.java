/**	
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
	
package org.apache.pig.piggybank.test.evaluation.string;	
	
import java.util.ArrayList;	
import java.util.List;	
import org.apache.pig.EvalFunc;	
import org.apache.pig.FuncSpec;	
import org.apache.pig.data.DataType;	
import org.apache.pig.data.Tuple;	
import org.apache.pig.backend.executionengine.ExecException;	
import org.apache.pig.impl.logicalLayer.FrontendException;	
import org.apache.pig.impl.logicalLayer.schema.Schema;	
	
/**	
 * Pig UDF to test input <code>tuple.get(0)</code> against <code>tuple.get(1)</code>	
 * to determine if the first argument ends with the string in the second.	
 */	
public class ENDSWITH extends EvalFunc<Boolean> throws ExecException{	
    @Override	
    public Boolean exec(Tuple tuple) {	
        if (tuple == null || tuple.size() != 2) {	
            return null;	
        }	
        String argument = null;	
        String testAgainst = null;	
        argument = (String) tuple.get(0);	
        testAgainst = (String) tuple.get(1);	
        return argument.endsWith(testAgainst);	
        	
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
