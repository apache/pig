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
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestPigSplit extends PigExecTestCase {
		
	@Test
	public void notestLongEvalSpec() throws Exception{
		File f = File.createTempFile("tmp", "");
		
		PrintWriter pw = new PrintWriter(f);
		pw.println("0\ta");
		pw.close();
		
		pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(f.toString()) + "';");
		for (int i=0; i< 500; i++){
			pigServer.registerQuery("a = filter a by $0 == '1';");
		}
		Iterator<Tuple> iter = pigServer.openIterator("a");
		while (iter.hasNext()){
			throw new Exception();
		}
		f.delete();
	}
	
    @Test
    public void testSchemaWithSplit() throws Exception {
        File f = File.createTempFile("tmp", "");

        PrintWriter pw = new PrintWriter(f);
        pw.println("2");
        pw.println("12");
        pw.println("42");
        pw.close();
        pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(f.toString()) + "' as (value);");
        pigServer.registerQuery("split a into b if value < 20, c if value > 10;");
        pigServer.registerQuery("b1 = order b by value;");
        pigServer.registerQuery("c1 = order c by value;");

        // order in lexicographic, so 12 comes before 2
        Iterator<Tuple> iter = pigServer.openIterator("b1");
        assertTrue("b1 has an element", iter.hasNext());
        assertEquals("first item in b1", iter.next().getAtomField(0).longVal(), 12);
        assertTrue("b1 has an element", iter.hasNext());
        assertEquals("second item in b1", iter.next().getAtomField(0).longVal(), 2);
        assertFalse("b1 is over", iter.hasNext());

        iter = pigServer.openIterator("c1");
        assertTrue("c1 has an element", iter.hasNext());
        assertEquals("first item in b1", iter.next().getAtomField(0).longVal(), 12);
        assertTrue("c1 has an element", iter.hasNext());
        assertEquals("second item in b1", iter.next().getAtomField(0).longVal(), 42);
        assertFalse("c1 is over", iter.hasNext());

        f.delete();
    }

}
