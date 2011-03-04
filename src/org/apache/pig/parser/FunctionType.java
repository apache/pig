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
package org.apache.pig.parser;

import org.apache.pig.ComparisonFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigToStream;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StreamToPig;

class FunctionType {
    public static final byte UNKNOWNFUNC = 0;
    public static final byte EVALFUNC = 2;
    public static final byte COMPARISONFUNC = 4;
    public static final byte LOADFUNC = 8; 
    public static final byte STOREFUNC = 16;
    public static final byte PIGTOSTREAMFUNC = 32;
    public static final byte STREAMTOPIGFUNC = 64;

    public static void tryCasting(Object func, byte funcType) throws Exception {
        switch(funcType) {
        case FunctionType.EVALFUNC:
            EvalFunc evalFunc = (EvalFunc) func;
            break;
        case FunctionType.COMPARISONFUNC:
            ComparisonFunc comparisonFunc = (ComparisonFunc) func;
            break;
        case FunctionType.LOADFUNC:
            LoadFunc loadFunc = (LoadFunc) func;
            break;
        case FunctionType.STOREFUNC:
            StoreFuncInterface storeFunc = (StoreFuncInterface) func;
            break;
        case FunctionType.PIGTOSTREAMFUNC:
            PigToStream ptsFunc = (PigToStream) func;
            break;
        case FunctionType.STREAMTOPIGFUNC:
            StreamToPig stpFunc = (StreamToPig) func;
            break;
        default:
            throw new Exception("Received an unknown function type: " + funcType);
        }
    }
    
};

