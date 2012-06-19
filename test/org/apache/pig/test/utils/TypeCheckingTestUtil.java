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

package org.apache.pig.test.utils;

import static org.apache.pig.newplan.logical.relational.LOTestHelper.newLOLoad;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

public class TypeCheckingTestUtil {

    public static org.apache.pig.newplan.logical.relational.LOLoad 
    genDummyLOLoadNewLP( org.apache.pig.newplan.logical.relational.LogicalPlan plan) throws ExecException  {
        String pigStorage = PigStorage.class.getName() ;
        PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
        pc.connect();
        org.apache.pig.newplan.logical.relational.LOLoad load =
        newLOLoad(
                new FileSpec("pi", new FuncSpec(pigStorage)),
                null, plan, new Configuration(ConfigurationUtil.toConfiguration(pc.getFs().getConfiguration()))
        );
        return load ;
    }
    

    public static Schema genFlatSchema(String[] aliases, byte[] types) {
        if (aliases.length != types.length) {
            throw new AssertionError(" aliase number and type number don't match") ;
        }
        List<Schema.FieldSchema> fsList = new ArrayList<Schema.FieldSchema>() ;
        for(int i=0; i<aliases.length ;i++) {
            fsList.add(new Schema.FieldSchema(aliases[i], types[i])) ;
        }
        return new Schema(fsList) ;
    }
    
    public static Schema genFlatSchemaInTuple(String[] aliases, byte[] types) {
        Schema flatSchema = genFlatSchema(aliases, types);
        return new Schema(new Schema.FieldSchema("t", flatSchema));
        
    }
    

    public static OperatorKey genNewOperatorKey() {
        long newId = NodeIdGenerator.getGenerator().getNextNodeId("scope") ;
        return new OperatorKey("scope", newId) ;
    }

    public static void printTypeGraph(LogicalPlan plan) {
        System.out.println("*****Type Graph*******") ;
        String rep = plan.toString() ;
        System.out.println(rep) ;
    }

    public static void printMessageCollector(CompilationMessageCollector collector) {
        if (collector.hasMessage()) {
            System.out.println("*****MessageCollector dump*******") ;
            Iterator<CompilationMessageCollector.Message> it1 = collector.iterator() ;
            while (it1.hasNext()) {
                CompilationMessageCollector.Message msg = it1.next() ;
                System.out.println(msg.getMessageType() + ":" + msg.getMessage());
            }
        }
    }

    public static void printCurrentMethodName() {
       StackTraceElement e[] = Thread.currentThread().getStackTrace() ;
       boolean doNext = false;
       for (StackTraceElement s : e) {
           if (doNext) {
              System.out.println(s.getMethodName());
              return;
           }
           doNext = s.getMethodName().equals("printCurrentMethodName");
       }
    }

    public static String getCurrentMethodName() {
       StackTraceElement e[] = Thread.currentThread().getStackTrace() ;
       boolean doNext = false;
       for (StackTraceElement s : e) {
           if (doNext) {
              return s.getMethodName();
           }
           doNext = s.getMethodName().equals("getCurrentMethodName");
       }
       return null;
    }

}
