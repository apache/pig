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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.DateTime;
import org.junit.Test;

public class TestInUdf {
    private static IN in = new IN();

    /**
     * Verify that IN EvalFunc works with various types of arguments.
     * @throws IOException
     */
    @Test
    public void testDataType() throws IOException {
        Object[][] args = {
                { new DataByteArray(new byte[] {'0'}),
                  new DataByteArray(new byte[] {'1'}),
                },
                { new Boolean(true),
                  new Boolean(false),
                },
                { new Integer(0),
                  new Integer(1),
                },
                { new Long(0l),
                  new Long(1l),
                },
                { new Float(0f),
                  new Float(1f),
                },
                { new Double(0d),
                  new Double(1d),
                },
                { new DateTime(0l),
                  new DateTime(1l),
                },
                { new String("0"),
                  new String("1"),
                },
                { new BigInteger("0"),
                  new BigInteger("1"),
                },
                { new BigDecimal("0.0"),
                  new BigDecimal("1.0"),
                },
        };

        for (int i = 0; i < args.length; i++) {
            Tuple input = TupleFactory.getInstance().newTuple();
            input.append(args[i][0]);
            input.append(args[i][1]);
            // x IN (y)
            assertFalse(in.exec(input));
            input.append(args[i][0]);
            // x IN (x, y)
            assertTrue(in.exec(input));
        }
    }

    /**
     * Verify that IN EvalFunc returns false when first argument is null.
     * @throws IOException
     */
    @Test
    public void testNull() throws IOException {
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append(null);
        input.append(null);
        // null IN (null)
        assertFalse(in.exec(input));
    }

    /**
     * Verify that IN EvalFunc throws ExecException with an invalid number of
     * arguments.
     * @throws IOException
     */
    @Test(expected = ExecException.class)
    public void testInvalidNumOfArgs() throws IOException {
        Tuple input = TupleFactory.getInstance().newTuple();
        input.append(new Object());
        // x IN ()
        in.exec(input);
        fail("IN must throw ExecException for " + input.size() + " argument(s)");
    }
}
