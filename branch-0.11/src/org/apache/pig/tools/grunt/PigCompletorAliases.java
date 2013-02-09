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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.PigServer;

import jline.Completor;

public class PigCompletorAliases implements Completor {
    private final Log log = LogFactory.getLog(getClass());
    Set<String> keywords;
    PigServer pig;

    public PigCompletorAliases(PigServer server)
    {
        keywords = new TreeSet<String>();
        pig = server;
        try {
            InputStream keywordStream = getClass().getResourceAsStream("/org/apache/pig/tools/grunt/autocomplete_aliases");
            PigCompletor.loadCandidateKeywords(keywordStream, keywords);
        }
        catch (IOException e) {
            log.warn("Error occurs when reading internal autocomplete_aliases file, skipped");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public int complete(String buffer, int cursor, List candidates)
    {
        if (cursor == 0)
            return -1;

        String[] tokens = buffer.split("\\s+");
        if (tokens.length==0)
            return -1;
        if (keywords.contains(tokens[tokens.length-1])) {
            for (String c : pig.getAliasKeySet()) {
                candidates.add(c);
            }
            return cursor;
        }

        if (tokens.length==1)
            return -1;

        if (!keywords.contains(tokens[tokens.length-2]))
            return -1;

        if (keywords.isEmpty()) {
            return cursor;
        }

        int p = cursor;
        p--;
        while (p > 0) {
            char c = buffer.charAt(p);
            if (Character.isWhitespace(c)) {
                p++;
                break;
            }
            p--;
        }
        StringBuffer sb = new StringBuffer();
        for (int i = p; i < cursor; i++) {
            sb.append(buffer.charAt(i));
        }
        if (!sb.toString().equals("")) {
            List<String> matches = searchCandidate(sb.toString());
            if (matches != null) {
                candidates.addAll(matches);
                return p;
            }
        }
        return -1;
    }

    private List<String> searchCandidate(String s)
    {
        List<String> list = new ArrayList<String>();
        for (String can : pig.getAliasKeySet()) {
            if (can.startsWith(s) && !can.equals(s))
                list.add(can);
        }
        return list;
    }
}
