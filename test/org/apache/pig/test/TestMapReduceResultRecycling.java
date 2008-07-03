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
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestMapReduceResultRecycling extends PigExecTestCase {
    
    @Test
    public void testPlanRecycling() throws Throwable {
        File tmpFile = this.createTempFile();
        {            
            String query = "a = load 'file:" + Util.encodeEscape(tmpFile.toString()) + "'; " ;
            System.out.println(query);
            pigServer.registerQuery(query);
            pigServer.explain("a", System.out) ;
            Iterator<Tuple> it = pigServer.openIterator("a");
            assertTrue(it.next().getAtomField(0).strval().equals("a1")) ;
            assertTrue(it.next().getAtomField(0).strval().equals("b1")) ;
            assertTrue(it.next().getAtomField(0).strval().equals("c1")) ;
            assertFalse(it.hasNext()) ;
        }
       
        {
            String query = "b = filter a by $0 eq 'a1';" ;
            System.out.println(query);
            pigServer.registerQuery(query);
            pigServer.explain("b", System.out) ;
            Iterator<Tuple> it = pigServer.openIterator("b");
            assertTrue(it.next().getAtomField(0).strval().equals("a1")) ;
            assertFalse(it.hasNext()) ;
        }
        
        {
            String query = "c = filter a by $0 eq 'b1';" ;
            System.out.println(query);
            pigServer.registerQuery(query);
            pigServer.explain("c", System.out) ;
            Iterator<Tuple> it = pigServer.openIterator("c");
            assertTrue(it.next().getAtomField(0).strval().equals("b1")) ;
            assertFalse(it.hasNext()) ;
        }
        
    }
    
    private File createTempFile() throws Throwable {
        File tmpFile = File.createTempFile("pi_test1", "txt");
        tmpFile.deleteOnExit() ;
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        ps.println("a1\t1\t1000") ;
        ps.println("b1\t2\t1000") ;
        ps.println("c1\t3\t1000") ;
        ps.close(); 
        return tmpFile ;
    }
}
