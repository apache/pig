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
package org.apache.pig.builtin;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.FilterFunc;
import org.apache.pig.PigException;
import org.apache.pig.TerminatingAccumulator;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;


/**
 * Determine whether a bag or map is empty.
 */
public class IsEmpty extends FilterFunc implements TerminatingAccumulator<Boolean> {

    private boolean isEmpty = true;

    @Override
    public Boolean exec(Tuple input) throws IOException {
        try {
            Object values = input.get(0);        
            if (values instanceof DataBag)
                return ((DataBag)values).size() == 0;
            else if (values instanceof Map)
                return ((Map)values).size() == 0;
            else {
                int errCode = 2102;
                String msg = "Cannot test a " +
                DataType.findTypeName(values) + " for emptiness.";
                throw new ExecException(msg, errCode, PigException.BUG);
            }
        } catch (ExecException ee) {
            throw ee;
        }
    }

    @Override
    public boolean isFinished() {
        return !isEmpty;
    }

    @Override
    public void accumulate(Tuple b) throws IOException {
        isEmpty &= exec(b);
    }

    @Override
    public void cleanup() {
        isEmpty = true;
    }

    @Override
    public Boolean getValue() {
        return isEmpty;
    }

}
