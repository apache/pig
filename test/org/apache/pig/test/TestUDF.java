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
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigServer;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;

public class TestUDF extends TestCase{
	
	
	public void test() throws Exception{
        int LOOP_COUNT = 4*10;
		MiniCluster cluster=MiniCluster.buildCluster();
		PigServer server = new PigServer(ExecType.MAPREDUCE,cluster.getProperties());
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i);
        }
        ps.close();
		server.registerQuery("A = load 'file:" + Util.encodeEscape(tmpFile.toString()) + "' USING "+ PigStorage.class.getName() + "(':');");
//		server.registerQuery("define c Test('a');");
        server.registerFunction("c", Test.class.getName() + "('a')" );
        server.registerQuery("C = group A by $0;");
		server.registerQuery("B = foreach C generate group, c($0);");
		Iterator<Tuple> result = server.openIterator("B");
		while (result.hasNext()) {
			   Tuple t = (Tuple) result.next();
			   System.out.println(t);
			}
	}
	

    static public class Test extends EvalFunc<DataAtom> implements Algebraic{
        protected String schemaName;
        
        public Test(){}
        
        
        public Test(String  schemaName){
                this.schemaName=schemaName;
        }
    
    
        @Override
        public void exec(Tuple input, DataAtom output) throws IOException {
            output.setValue(schemaName);
        }
    
        public String toString() {
                return "('"+schemaName+"')";
        }
    
        public String getFinal() {
            // TODO Auto-generated method stub
            System.out.println("Final Called ************************************");
            return Final.class.getName() + toString();
        }
    
    
        public String getInitial() {
            // TODO Auto-generated method stub
            return Initial.class.getName();
        }
    
    
        public String getIntermed() {
            // TODO Auto-generated method stub
            return Initial.class.getName();
        }
        
        static public class Initial extends EvalFunc<Tuple> {
            @Override
            public void exec(Tuple input, Tuple output) throws IOException {
            }
        }
        static public class Final extends EvalFunc<DataAtom> {
            String schemaName;
            public Final(){}
            
            public Final(String schemaName){
                this.schemaName = schemaName;
            }
            
            @Override
            public void exec(Tuple input, DataAtom output) throws IOException {
                output.setValue(schemaName);
            }
        }
        
        
    }
}
