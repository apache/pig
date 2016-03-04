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

package org.apache.pig.piggybank.storage.apachelog;

import nl.basjes.pig.input.apachehttpdlog.Loader;

/**
 * This is a pig loader that can load Apache HTTPD access logs written in (almost) any
 * Apache HTTPD LogFormat.<br/>
 * Basic usage: <br/>
 * Simply feed the loader your (custom) logformat specification and it will tell you which fields
 * can be extracted from this logformat.<br/>
 * For example:
 * <pre>
 * -- Specify any existing file as long as it exists.
 * -- It won't be read by the loader when no fields are requested.
 * Example =
 *     LOAD 'test.pig'
 *     USING org.apache.pig.piggybank.storage.apachelog.LogFormatLoader(
 *       '%h %l %u %t "%r" %>s %b "%{Referer}i" "%{User-Agent}i"'
 *     );
 * DUMP Example;
 * </pre>
 *
 * The output of this command is a (huge) example (yes actual pig code) which demonstrates
 * how all possible fields can be extracted. In normal use cases this example will be trimmed
 * down to request only the fields your application really needs.
 * This loader implements pushdown projection so there is no need to worry too much about the
 * fields you leave in.
 * This loader supports extracting things like an individual cookie or query string parameter
 * regardless of the position it has in the actual log line.
 *
 * In addition to the logformat specification used in your custom config this parser also
 * understands the standard formats:<pre>
 *    common
 *    combined
 *    combinedio
 *    referer
 *    agent
 * </pre>
 *
 * So this works also:
 * <pre>
 * Example =
 *     LOAD 'test.pig'
 *     USING org.apache.pig.piggybank.storage.apachelog.LogFormatLoader('common');
 * DUMP Example;
 * </pre>
 *
 * This class is simply a wrapper around <a href="https://github.com/nielsbasjes/logparser"
 * >https://github.com/nielsbasjes/logparser</a> so more detailed documentation can be found there.
 */
public class LogFormatLoader extends Loader {
    public LogFormatLoader(String... parameters) {
        super(parameters);
    }
}
