
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

package org.apache.pig.scripting;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.impl.PigContext;

/**
 * Context for embedded Pig script. 
 */
public class ScriptPigContext {
    
    private static ThreadLocal<ScriptPigContext> tss = new ThreadLocal<ScriptPigContext>();
        
    private PigContext pigContext;
    
    private ScriptEngine engine;
    
    public static ScriptPigContext set(PigContext pigContext,
            ScriptEngine engine) {
        tss.set(new ScriptPigContext(pigContext, engine));
        return tss.get();
    }
    
    public static ScriptPigContext get() {
        return tss.get();
    }
    
    public ScriptEngine getScriptEngine() {
        return engine;
    }

    public PigContext getPigContext() {
        return pigContext;
    }
    
    //-------------------------------------------------------------------------
    
    private ScriptPigContext(PigContext pigContext, ScriptEngine engine) {
        this.pigContext = pigContext;
        this.engine = engine;       
    }
}
