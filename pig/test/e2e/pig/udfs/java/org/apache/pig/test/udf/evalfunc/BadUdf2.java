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

import java.io.*;
import java.text.*;
import java.util.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.backend.executionengine.ExecException;

public class BadUdf2 extends EvalFunc<DataBag>
{
    public DataBag exec(Tuple input) throws IOException
    {
        try
        {
            DataBag bag = BagFactory.getInstance().newDefaultBag();

            Tuple t = DefaultTupleFactory.getInstance().newTuple(2); // BUG BUG: Notice how there are supposed to be 2
//fields but then 3 fields are written out below. 
            t.set(0,"a");
            t.set(1,"b");
            t.set(2,"c");

            Tuple s = DefaultTupleFactory.getInstance().newTuple(2);
            s.set(0,"A");
            s.set(1,"B");
            s.set(2,"C");

            bag.add(t);
            bag.add(s);

            return bag;
        }
        catch (ExecException e)
        {
            throw e;
        }
    }
}
