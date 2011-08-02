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
package org.apache.pig.penny;

import java.io.IOException;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;

public class PennyServer {
    
    private final PigContext pigContext;
    private static ExecType execType = ExecType.MAPREDUCE;
    private static Properties properties = new Properties();
    public static void setExecType(ExecType execType) {
        PennyServer.execType = execType;
    }
    public static void setProperties(Properties properties) {
        PennyServer.properties = properties;
    }
    
    public PennyServer() throws ExecException {
        pigContext = new PigContext(execType, properties);
    }

    public ParsedPigScript parse(String pigScriptFilename) throws IOException {
        return new ParsedPigScript(pigContext, pigScriptFilename);
    }
    
    public PigContext getPigContext() {
        return pigContext;
    }

}
