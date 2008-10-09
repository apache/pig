/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.test.storage;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.pig.PigServer.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.piggybank.storage.RegExLoader;
import org.junit.Test;

public class TestRegExLoader extends TestCase {
    private static String patternString = "(\\w+),(\\w+);(\\w+)";
    private final static Pattern pattern = Pattern.compile(patternString);

    class DummyRegExLoader extends RegExLoader {
        @Override
        public Pattern getPattern() {
            return Pattern.compile(patternString);
        }
    }

    public static ArrayList<String[]> data = new ArrayList<String[]>();
    static {
        data.add(new String[] { "1,one;i" });
        data.add(new String[] { "2,two;ii" });
        data.add(new String[] { "3,three;iii" });
    }

    @Test
    public void testLoadFromBindTo() throws Exception {
        String filename = TestHelper.createTempFile(data, " ");
        DummyRegExLoader dummyRegExLoader = new DummyRegExLoader();
        PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
        InputStream inputStream = FileLocalizer.open(filename, pigContext);
        dummyRegExLoader.bindTo(filename, new BufferedPositionedInputStream(inputStream), 0, Long.MAX_VALUE);

        ArrayList<String[]> expected = TestHelper.getExpected(data, pattern);
        int tupleCount = 0;

        while (true) {
            Tuple tuple = dummyRegExLoader.getNext();
            if (tuple == null)
                break;
            else {
                TestHelper.examineTuple(expected, tuple, tupleCount);
                tupleCount++;
            }
        }
        assertEquals(data.size(), tupleCount);
    }
}
