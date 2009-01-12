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

import org.apache.pig.FuncSpec;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.test.TypeGraphPrinter;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class TypeCheckingTestUtil {

    public static LOLoad genDummyLOLoad(LogicalPlan plan)  {
        String pigStorage = PigStorage.class.getName() ;
        try {
            LOLoad load = new LOLoad(plan,
                                      genNewOperatorKey(),
                                      new FileSpec("pi", new FuncSpec(pigStorage)),
                                      null, null, true) ;
            return load ;
        } catch (IOException e) {
            throw new AssertionError("This cannot happen") ;
        }
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

    public static OperatorKey genNewOperatorKey() {
        long newId = NodeIdGenerator.getGenerator().getNextNodeId("scope") ;
        return new OperatorKey("scope", newId) ;
    }

    public static void printTypeGraph(LogicalPlan plan) {
        System.out.println("*****Type Graph*******") ;
        TypeGraphPrinter printer = new TypeGraphPrinter(plan) ;
        String rep = printer.printToString() ;
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
