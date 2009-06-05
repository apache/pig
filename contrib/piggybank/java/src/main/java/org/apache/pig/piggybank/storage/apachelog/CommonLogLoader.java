/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.storage.apachelog;

import java.util.regex.Pattern;

import org.apache.pig.piggybank.storage.RegExLoader;

/**
 * CommonLogLoader is used to load logs based on Apache's common log format, based on a format like
 * 
 * LogFormat "%h %l %u %t \"%r\" %>s %b" common
 * 
 * The log filename ends up being access_log from a line like
 * 
 * CustomLog logs/access_log common
 * 
 * Example:
 * 
 * raw = LOAD 'access_log' USING org.apache.pig.piggybank.storage.apachelog.CommongLogLoader AS (remoteAddr,
 * remoteLogname, user, time, method, uri, proto, bytes);
 * 
 */

public class CommonLogLoader extends RegExLoader {
    // 81.19.151.110 - - [04/Oct/2008:13:28:23 -0600] "GET / HTTP/1.0" 200 156
    private final static Pattern commonLogPattern = Pattern
        .compile("^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+.(\\S+\\s+\\S+).\\s+.(\\S+)\\s+(\\S+)\\s+(\\S+.\\S+).\\s+(\\S+)\\s+(\\S+)$");

    public Pattern getPattern() {
        return commonLogPattern;
    }
}
