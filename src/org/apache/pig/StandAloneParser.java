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
package org.apache.pig;

import java.io.*;
import java.util.*;

import org.apache.pig.PigServer.ExecType;
import org.apache.pig.impl.logicalLayer.LogicalPlan;

import org.apache.pig.backend.executionengine.ExecException;

public class StandAloneParser {
    
    static PigServer pig;
    
    public static void main(String args[]) throws IOException, ExecException {
        
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        pig = new PigServer(ExecType.LOCAL);
        
        while (true) {
            System.out.print("> ");
            
            String line;
            try {
                line = in.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            
            if (line.toLowerCase().equals("quit")) break;
            if (line.toLowerCase().startsWith("#")) continue;
            else tryParse(line);
            
        }
        
           
    }
    
    private static void tryParse(String query) {
        if (query.trim().equals(""))
            return;
        try{        
            pig.registerQuery(query);
            System.out.print("Current aliases: ");
            for (Iterator<String> it = pig.getAliases().keySet().iterator(); it.hasNext(); ) {
                String alias = it.next();
                LogicalPlan lp = pig.getAliases().get(alias);
                System.out.print(alias + "->" + lp.getOpTable().get(lp.getRoot()).outputSchema());
                if (it.hasNext()) System.out.print(", \n");
                else System.out.print("\n");
            }
        } catch (IOException e) {
            System.err.println(e);
        }
    }
}
