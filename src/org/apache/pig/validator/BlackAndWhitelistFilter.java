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
package org.apache.pig.validator;

import java.util.Set;

import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.python.google.common.base.Splitter;
import org.python.google.common.collect.Sets;

/**
 * 
 * This Filter handles black and whitelisting of Pig commands.
 */
public final class BlackAndWhitelistFilter implements PigCommandFilter {
    private static final int BLACKANDWHITELIST_ERROR_CODE = 1856;
    private static final Splitter SPLITTER = Splitter.on(',').trimResults()
            .omitEmptyStrings();

    private final PigServer pigServer;
    private final Set<String> whitelist;
    private final Set<String> blacklist;

    public BlackAndWhitelistFilter(PigServer pigServer) {
        this.pigServer = pigServer;
        whitelist = Sets.newHashSet();
        blacklist = Sets.newHashSet();

        init();
    }

    private void init() {
        PigContext context = pigServer.getPigContext();
        String whitelistConfig = context.getProperties().getProperty(PigConfiguration.PIG_WHITELIST);

        if (whitelistConfig != null) {
            Iterable<String> iter = SPLITTER.split(whitelistConfig);
            for (String elem : iter) {
                whitelist.add(elem.toUpperCase());
            }
        }

        String blacklistConfig = context.getProperties().getProperty(PigConfiguration.PIG_BLACKLIST);
        if (blacklistConfig != null) {
            Iterable<String> iter = SPLITTER.split(blacklistConfig);
            for(String elem : iter) {
                String uElem = elem.toUpperCase();
                if(whitelist.contains(uElem)) {
                    throw new IllegalStateException("Conflict between whitelist and blacklist. '"+elem+"' appears in both.");
                }
                blacklist.add(uElem);
            }
        }
    }

    @Override
    public void validate(Command command) throws FrontendException {
        if (blacklist.contains(command.name())) {
            throw new FrontendException(command.name() + " command is not permitted. ", BLACKANDWHITELIST_ERROR_CODE);
        }

        // check for size of whitelist as an empty whitelist should not disallow using Pig commands altogether.
        if (whitelist.size() > 0 && !whitelist.contains(command.name())) {
            throw new FrontendException(command.name() + " command is not permitted. ", BLACKANDWHITELIST_ERROR_CODE);
        }
    }

}
