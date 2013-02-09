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

package org.apache.pig.piggybank.evaluation;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * MaxTupleBy1stField UDF returns a tuple with max value of the first field in a
 * given bag.
 *
 * Caveat: first field assumed to have type 'long'. You may need to enforece this
 * via schema when loading data, as sown in sample usage below.
 * 
 * Sample usage: 
 * 
 * A = load 'test.tsv' as (first: long, second, third); 
 * B = GROUP A by second; 
 * C = FOREACH B GENERATE group, MaxTupleBy1stField(A);
 * 
 * @author Vadim Zaliva <lord@codemindes.com>
 */
public class MaxTupleBy1stField extends EvalFunc<Tuple> implements Algebraic
{
    /**
     * Indicates once for how many items progress hartbeat should be sent.
     */
    private static final int PROGRESS_FREQUENCY = 10;

    static public class Initial extends EvalFunc<Tuple>
    {
        //TODO: private static TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException
        {
            try
            {
                // input is a bag with one tuple containing
                // the column we are trying to max on
                DataBag bg = (DataBag) input.get(0);
                Tuple tp = bg.iterator().next();
                return tp; //TODO: copy?
            } catch(ExecException ee)
            {
                IOException oughtToBeEE = new IOException();
                oughtToBeEE.initCause(ee);
                throw oughtToBeEE;
            }
        }
    }

    public Schema outputSchema(Schema input) 
    {
        return input;
    }

    static public class Intermediate extends EvalFunc<Tuple>
    {
        //TODO: private static TupleFactory tfact = TupleFactory.getInstance();

        @Override
        public Tuple exec(Tuple input) throws IOException
        {
            try
            {
                return max(input, reporter);
            } catch(ExecException ee)
            {
                IOException oughtToBeEE = new IOException();
                oughtToBeEE.initCause(ee);
                throw oughtToBeEE;
            }
        }
    }

    static public class Final extends EvalFunc<Tuple>
    {
        @Override
        public Tuple exec(Tuple input) throws IOException
        {
            try
            {
                return max(input, reporter);
            } catch(ExecException ee)
            {
                IOException oughtToBeEE = new IOException();
                oughtToBeEE.initCause(ee);
                throw oughtToBeEE;
            }
        }
    }

    @Override
    public Tuple exec(Tuple input) throws IOException
    {
        try
        {
            return max(input, reporter);
        } catch(ExecException ee)
        {
            IOException oughtToBeEE = new IOException();
            oughtToBeEE.initCause(ee);
            throw oughtToBeEE;
        }
    }

    protected static Tuple max(Tuple input, PigProgressable reporter) throws ExecException
    {
        DataBag values = (DataBag) input.get(0);

        // if we were handed an empty bag, return NULL
        // this is in compliance with SQL standard
        if(values.size() == 0)
            return null;

        long curMax = 0;
        Tuple curMaxTuple = null;
        int n=0;
        for(Iterator<Tuple> it = values.iterator(); it.hasNext();)
        {
            if(reporter!=null && ++n%PROGRESS_FREQUENCY==0)
                reporter.progress();
            Tuple t = it.next();
            try
            {
                long d = (Long) t.get(0);
                if(curMaxTuple == null || d > curMax)
                {
                    curMax = d;
                    curMaxTuple = t;
                }

            } catch(RuntimeException exp)
            {
                ExecException newE = new ExecException("Error processing: " + t.toString() + exp.getMessage());
                newE.initCause(exp);
                throw newE;
            }
        }

        return curMaxTuple;
    }

    @Override
    public String getInitial()
    {
        return Initial.class.getName();
    }

    @Override
    public String getIntermed()
    {
        return Intermediate.class.getName();
    }

    @Override
    public String getFinal()
    {
        return Final.class.getName();
    }

}
