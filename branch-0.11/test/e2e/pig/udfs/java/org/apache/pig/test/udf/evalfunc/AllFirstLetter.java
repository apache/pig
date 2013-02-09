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
import java.util.Iterator;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class AllFirstLetter extends EvalFunc<String> implements Accumulator<String>
{
    // @Override
    public String exec(Tuple input) throws IOException {
        result = "";
        DataBag bag = (DataBag) input.get(0);
        Iterator<Tuple> it = bag.iterator();
        while (it.hasNext()) {
            Tuple t = it.next();
            if (t != null && t.size() > 0 && t.get(0) != null)
                result += t.get(0).toString().substring(0, 1);
        }
        return result;
    }

    /* Accumulator interface implementation */
    String result = "";
    @Override
    public void accumulate(Tuple b) throws IOException {
        try {
            DataBag bag = (DataBag) b.get(0);
            Iterator it = bag.iterator();
            while (it.hasNext()) {
                Tuple t = (Tuple) it.next();
                if (t != null && t.size() > 0 && t.get(0) != null) {
                    result += t.get(0).toString().substring(0, 1);
                }
            }
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing AllFirstLetter in "
                    + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void cleanup() {
        result = "";
    }

    @Override
    public String getValue() {
        return result;
    }
}
