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
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader;
import org.junit.Test;

public class TestCombinedLogLoader extends TestCase {
    public static ArrayList<String[]> data = new ArrayList<String[]>();
    static {
        data.add(new String[] { "1.2.3.4", "-", "-", "[01/Jan/2008:23:27:45 -0600]", "\"GET /zero.html HTTP/1.0\"", "200", "100", "\"-\"",
            "\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"" });
        data.add(new String[] { "1.2.3.4", "-", "-", "[01/Jan/2008:23:27:45 -0600]", "\"GET /zero.html HTTP/1.0\"", "200", "100",
            "\"http://myreferringsite.com\"",
            "\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"" });
        data.add(new String[] { "1.2.3.4", "-", "-", "[01/Jan/2008:23:27:45 -0600]", "\"GET /zero.html HTTP/1.0\"", "200", "100", "\"-\"",
            "\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1\"" });
    }

    public static ArrayList<String[]> EXPECTED = new ArrayList<String[]>();
    static {

        for (int i = 0; i < data.size(); i++) {
            ArrayList<String> thisExpected = new ArrayList<String>();
            for (int j = 0; j <= 2; j++) {
                thisExpected.add(data.get(i)[j]);
            }
            String temp = data.get(i)[3];
            temp = temp.replace("[", "");
            temp = temp.replace("]", "");
            thisExpected.add(temp);

            temp = data.get(i)[4];

            for (String thisOne : data.get(i)[4].split(" ")) {
                thisOne = thisOne.replace("\"", "");
                thisExpected.add(thisOne);
            }
            for (int j = 5; j <= 6; j++) {
                thisExpected.add(data.get(i)[j]);
            }
            for (int j = 7; j <= 8; j++) {
                String thisOne = data.get(i)[j];
                thisOne = thisOne.replace("\"", "");
                thisExpected.add(thisOne);
            }

            String[] toAdd = new String[0];
            toAdd = (String[]) (thisExpected.toArray(toAdd));
            EXPECTED.add(toAdd);
        }
    }

    @Test
    public void testInstantiation() {
        CombinedLogLoader combinedLogLoader = new CombinedLogLoader();
        assertNotNull(combinedLogLoader);
    }

    @Test
    public void testLoadFromBindTo() throws Exception {
        String filename = TestHelper.createTempFile(data, " ");
        CombinedLogLoader combinedLogLoader = new CombinedLogLoader();
        PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
        InputStream inputStream = FileLocalizer.open(filename, pigContext);
        combinedLogLoader.bindTo(filename, new BufferedPositionedInputStream(inputStream), 0, Long.MAX_VALUE);

        int tupleCount = 0;

        while (true) {
            Tuple tuple = combinedLogLoader.getNext();
            if (tuple == null)
                break;
            else {
                TestHelper.examineTuple(EXPECTED, tuple, tupleCount);
                tupleCount++;
            }
        }
        assertEquals(data.size(), tupleCount);
    }

    public void testLoadFromPigServer() throws Exception {
        String filename = TestHelper.createTempFile(data, " ");
        PigServer pig = new PigServer(ExecType.LOCAL);
        filename = filename.replace("\\", "\\\\");
        pig.registerQuery("A = LOAD 'file:" + filename + "' USING org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader();");
        Iterator<?> it = pig.openIterator("A");

        int tupleCount = 0;

        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
                break;
            else {
                TestHelper.examineTuple(EXPECTED, tuple, tupleCount);
                tupleCount++;
            }
        }
        assertEquals(data.size(), tupleCount);
    }
}
