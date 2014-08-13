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
package org.apache.pig.pigunit.pig;

import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Slightly modified GruntParser that accepts a list of aliases to override.
 *
 * <p>This is a way to replace a pig query by another query.
 *
 * <p>For example, if we have this map of overrides: Map&lt;alias,query&gt;
 * <ul>
 *   <li>&lt;A, A = LOAD '/path'&gt; --> replace the alias A by A = LOAD '/path'</li>
 *   <li>&lt;DUMP, &gt; --> remove the DUMP queries</li>
 * </ul>
 *
 * <p>It might be possible to do the same thing in a less hacky way.
 * e.g. pig.registerQuery replace the query of a certain alias...
 */
public class GruntParser extends org.apache.pig.tools.grunt.GruntParser {
    /** A mapping <alias,query> to apply to the pig script. */
    private final Map<String, String> aliasOverride;

    /**
     * Initializes the Pig parser with its list of aliases to override.
     * 
     * @param stream The Pig script stream.
     * @param aliasOverride The list of aliases to override in the Pig script.
     */
    public GruntParser(Reader stream, Map<String, String> aliasOverride) {
        this(stream, null, aliasOverride);
    }

    public GruntParser(Reader stream, PigServer pigServer, Map<String, String> aliasOverride) {
        super(stream, pigServer);
        this.aliasOverride = aliasOverride;
    }

    /**
     * Pig relations that have been blanked are dropped.
     */
    @Override
    protected void processPig(String cmd) throws IOException {
        String command = override(cmd);

        if (!command.equals("")) {
            super.processPig(command);
        }
    }

    /**
     * Overrides the relations of the pig script that we want to change.
     * 
     * @param query
     *            The current pig query processed by the parser.
     * @return The same query, or a modified query, or blank.
     */
    public String override(String query) {
        // a path to be prepended to all the file names in the script
        String fsRoot = System.getProperty("pigunit.filesystem.prefix");
        if (fsRoot != null) {
            query = Pattern.compile("(LOAD\\s+'(([^:/?#]+)://)?)", Pattern.CASE_INSENSITIVE).matcher(query).replaceFirst("$1" + fsRoot);
            query = Pattern.compile("(STORE\\s+([^']+)\\s+INTO\\s+'(([^:/?#]+)://)?)", Pattern.CASE_INSENSITIVE).matcher(query).replaceFirst("$1" + fsRoot);
        }

        Map<String, String> metaData = new HashMap<String, String>();

        for (Entry<String, String> alias : aliasOverride.entrySet()) {
            saveLastStoreAlias(query, metaData);

            if (query.toLowerCase().startsWith(alias.getKey().toLowerCase() + " ")) {
                System.out.println(String.format("%s\n--> %s", query, alias.getValue() == "" ? "none" : alias.getValue()));
                query = alias.getValue();
            }
        }

        aliasOverride.putAll(metaData);

        return query;
    }

    /**
     * Saves the name of the alias of the last store.
     * 
     * <p>
     * Maybe better to replace it by PigServer.getPigContext().getLastAlias().
     */
    void saveLastStoreAlias(String cmd, Map<String, String> metaData) {
        if (cmd.toUpperCase().startsWith("STORE")) {
            Pattern outputFile = Pattern.compile("STORE +([^']+) INTO.*", Pattern.CASE_INSENSITIVE);
            Matcher matcher = outputFile.matcher(cmd);
            if (matcher.matches()) {
                metaData.put("LAST_STORE_ALIAS", matcher.group(1));
            }
        }
    }
}
