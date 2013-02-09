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
 * CombinedLogLoader is used to load logs based on Apache's combined log format, based on a format like
 * 
 * LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
 * 
 * The log filename ends up being access_log from a line like
 * 
 * CustomLog logs/combined_log combined
 * 
 * Example:
 * 
 * raw = LOAD 'combined_log' USING org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader AS
 * (remoteAddr, remoteLogname, user, time, method, uri, proto, status, bytes, referer, userAgent);
 * 
 */

public class CombinedLogLoader extends RegExLoader {
    // 1.2.3.4 - - [30/Sep/2008:15:07:53 -0400] "GET / HTTP/1.1" 200 3190 "-"
    // "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_4; en-us) AppleWebKit/525.18 (KHTML, like Gecko) Version/3.1.2 Safari/525.20.1"
    private final static Pattern combinedLogPattern = Pattern
        .compile("^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+.(\\S+\\s+\\S+).\\s+\"(\\S+)\\s+(.+?)\\s+(HTTP[^\"]+)\"\\s+(\\S+)\\s+(\\S+)\\s+\"([^\"]*)\"\\s+\"(.*)\"$");

    public Pattern getPattern() {
        return combinedLogPattern;
    }
}
