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
package org.apache.pig.tools.grunt;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.backend.executionengine.ExecException;


public class Grunt 
{
    private final Log log = LogFactory.getLog(getClass());
    
    BufferedReader in;
    PigServer pig;
    GruntParser parser;    

    public Grunt(BufferedReader in, PigContext pigContext) throws ExecException
    {
        this.in = in;
        this.pig = new PigServer(pigContext);
        
        if (in != null)
        {
            parser = new GruntParser(in);
            parser.setParams(pig);    
        }
    }

    public void run() {
    parser.setInteractive(true);
    parser.parseContOnError();
    }

    public void exec() {
        try {
        parser.setInteractive(false);
        parser.parseStopOnError();
        } catch (Exception e) {
            Exception pe = Utils.getPermissionException(e);
            if (pe != null)
                log.error("You don't have permission to perform the operation. Error from the server: " + pe.getMessage());
            else {
                ByteArrayOutputStream bs = new ByteArrayOutputStream();
                e.printStackTrace(new PrintStream(bs));
                log.error(bs.toString());
                log.error(e.getMessage());
           }
    }
    
    }
}
