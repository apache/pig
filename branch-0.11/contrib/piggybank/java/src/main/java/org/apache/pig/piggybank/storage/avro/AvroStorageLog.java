/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.pig.piggybank.storage.avro;

/**
 *  Simple logging utils of this package
 */
public class AvroStorageLog {

    static private int debugLevel = -1;
    
    final static public int INFO = 2;
    final static public int FUNC_CALL = 3;
    final static public int DETAILS = 5;
    
    static void setDebugLevel (int l) {
        debugLevel = l;
    }
    
    static void info (String msg) {
        if (debugLevel >= INFO) 
            System.out.println("INFO:" + msg);
    }
    
    static void funcCall (String msg) {
        if (debugLevel >= FUNC_CALL) 
            System.out.println("DEBUG:" + msg);
    }
    
    static void details (String msg) {
        if (debugLevel >= DETAILS)
            System.out.println("DEBUG:" + msg);
    }
    
    static void debug(int level, String msg) {
        if (debugLevel >= level)
            System.out.println("DEBUG:" + msg);
    }
    
    static void warn( String msg) {
        System.out.println("WARNING:" + msg);
    } 
    
    static void error(String msg) {
        System.err.println("ERROR:" + msg);
    }
    
}

