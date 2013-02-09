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

package org.apache.pig.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestPigSplit extends PigExecTestCase {
	
    /**
     * filename of input in each of the tests
     */
    String inputFileName;
    
    public TestPigSplit() {
        // by default let's test in map-red mode, user can
        // override this by setting ant property "test.exectype"
        execType = ExecType.MAPREDUCE;
    }
    
    private void createInput(String[] data) throws IOException {
        if(execType == ExecType.MAPREDUCE) {
            Util.createInputFile(cluster, inputFileName, data);
        } else if (execType == ExecType.LOCAL) {
            Util.createLocalInputFile(inputFileName, data);
        } else {
            throw new IOException("unknown exectype:" + execType.toString());
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.test.PigExecTestCase#tearDown()
     */
    @Override
    public void tearDown() throws Exception {
        if(execType == ExecType.MAPREDUCE) {
            Util.deleteFile(cluster, inputFileName);
        } else if (execType == ExecType.LOCAL) {
            new File(inputFileName).delete();
        } else {
            throw new IOException("unknown exectype:" + execType.toString());
        }
    }
    
	public void notestLongEvalSpec() throws Exception{
		inputFileName = "notestLongEvalSpec-input.txt";
		createInput(new String[] {"0\ta"});
		
		pigServer.registerQuery("a = load '" + inputFileName + "';");
		for (int i=0; i< 500; i++){
			pigServer.registerQuery("a = filter a by $0 == '1';");
		}
		Iterator<Tuple> iter = pigServer.openIterator("a");
		while (iter.hasNext()){
			throw new Exception();
		}
	}
	
    @Test
    public void testSchemaWithSplit() throws Exception {
        inputFileName = "testSchemaWithSplit-input.txt";
        String[] input = {"2","12","42"};
        createInput(input);
        pigServer.registerQuery("a = load '" + inputFileName + "' as (value:chararray);");
        pigServer.registerQuery("split a into b if value < '20', c if value > '10';");
        pigServer.registerQuery("b1 = order b by value;");
        pigServer.registerQuery("c1 = order c by value;");

        // order in lexicographic, so 12 comes before 2
        Iterator<Tuple> iter = pigServer.openIterator("b1");
        assertTrue("b1 has an element", iter.hasNext());
        assertEquals("first item in b1", iter.next().get(0), "12");
        assertTrue("b1 has an element", iter.hasNext());
        assertEquals("second item in b1", iter.next().get(0), "2");
        assertFalse("b1 is over", iter.hasNext());

        iter = pigServer.openIterator("c1");
        assertTrue("c1 has an element", iter.hasNext());
        assertEquals("first item in c1", iter.next().get(0), "12");
        assertTrue("c1 has an element", iter.hasNext());
        assertEquals("second item in c1", iter.next().get(0), "2");
        assertTrue("c1 has an element", iter.hasNext());
        assertEquals("third item in c1", iter.next().get(0), "42");
        assertFalse("c1 is over", iter.hasNext());

    }

    @Test
    public void testLongEvalSpec() throws Exception{
        inputFileName = "testLongEvalSpec-input.txt";
        String[] input = new String[500];
        for (int i=0; i< 500; i++) {
            input[i] = ("0\ta");
        }
        createInput(input);        
        pigServer.registerQuery("a = load '" + inputFileName + "';");
        pigServer.registerQuery("a = filter a by $0 == '1';");

        Iterator<Tuple> iter = pigServer.openIterator("a");
        while (iter.hasNext()){
            throw new Exception();
        }
    }
}
