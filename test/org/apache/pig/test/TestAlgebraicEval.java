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

import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestAlgebraicEval extends PigExecTestCase {

    private int LOOP_COUNT = 1024;

    @Test
    public void testSimpleCount() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i);
        }
        ps.close();
        StringBuilder query = new StringBuilder();
        query.append("myid =  foreach (group (load 'file:");
        query.append(Util.encodeEscape(tmpFile.toString()));
        query.append("') all) generate COUNT($1);");
        if (log.isDebugEnabled()) {
            log.debug(query.toString());
        }
        pigServer.registerQuery(query.toString());
        Iterator it = pigServer.openIterator("myid");
        tmpFile.delete();
        Tuple t = (Tuple)it.next();
        Double count = t.getAtomField(0).numval();
        assertEquals(count, (double)LOOP_COUNT);
    }

    @Test
    public void testGroupCount() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i);
        }
        ps.close();
        StringBuilder query = new StringBuilder();
        query.append("myid = foreach (group (load 'file:");
        query.append(Util.encodeEscape(tmpFile.toString()));
        query.append("') all) generate group, COUNT($1) ;");
        if (log.isDebugEnabled()) {
            log.debug(query.toString());
        }
        pigServer.registerQuery(query.toString());
        Iterator it = pigServer.openIterator("myid");
        tmpFile.delete();
        Tuple t = (Tuple)it.next();
        Double count = t.getAtomField(1).numval();
        assertEquals(count, (double)LOOP_COUNT);
    }
    
    @Test
    public void testGroupReorderCount() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i);
        }
        ps.close();
        StringBuilder query = new StringBuilder();
        query.append("myid = foreach (group (load 'file:");
        query.append(Util.encodeEscape(tmpFile.toString()));
        query.append("') all) generate COUNT($1), group ;");
        if (log.isDebugEnabled()) {
            log.debug(query.toString());
        }
        pigServer.registerQuery(query.toString());
        Iterator it = pigServer.openIterator("myid");
        tmpFile.delete();
        Tuple t = (Tuple)it.next();
        Double count = t.getAtomField(0).numval();
        assertEquals(count, (double)LOOP_COUNT);
    }



    @Test
    public void testGroupUniqueColumnCount() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int groupsize = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i%10 == 0) groupsize++;
            ps.println(i%10 + ":" + i);
        }
        ps.close();
        String query = "myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) + "' using " + PigStorage.class.getName() + "(':')) by $0) generate group, COUNT($1.$1) ;";
        System.out.println(query);
        pigServer.registerQuery(query);
        Iterator it = pigServer.openIterator("myid");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            Double group = t.getAtomField(0).numval();
            if(group == 0.0) {
                Double count = t.getAtomField(1).numval();
                assertEquals(count, (double)groupsize);
                break;
            }
        }   
    }

    @Test
    public void testGroupDuplicateColumnCount() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int groupsize = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i%10 == 0) groupsize++;
            ps.println(i%10 + ":" + i);
        }
        ps.close();
        String query = "myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) + "' using " + PigStorage.class.getName() + "(':')) by $0) generate group, COUNT($1.$1), COUNT($1.$0) ;";
        System.out.println(query);
        pigServer.registerQuery(query);
        Iterator it = pigServer.openIterator("myid");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            Double group = t.getAtomField(0).numval();
            if(group == 0.0) {
                Double count = t.getAtomField(1).numval();
                assertEquals(count, (double)groupsize);
                break;
            }
        }
    }

}
