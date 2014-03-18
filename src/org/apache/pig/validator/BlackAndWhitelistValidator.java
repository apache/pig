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
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LOCube;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LONative;
import org.apache.pig.newplan.logical.relational.LORank;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.rules.LogicalRelationalNodeValidator;
import org.python.google.common.base.Splitter;
import org.python.google.common.collect.Sets;

/**
 * This validator walks through the list of operators defined in {@link PigConfiguration#PIG_BLACKLIST} and
 * {@link PigConfiguration#PIG_WHITELIST} and checks whether the operation is permitted. In case these
 * properties are not defined (default), we let everything pass as usual.
 *
 */
public final class BlackAndWhitelistValidator implements LogicalRelationalNodeValidator {
    private final PigContext pigContext;
    private final OperatorPlan operatorPlan;

    public BlackAndWhitelistValidator(PigContext pigContext, OperatorPlan operatorPlan) {
        this.pigContext = pigContext;
        this.operatorPlan = operatorPlan;
    }

    public void validate() throws FrontendException {
        BlackAndWhitelistVisitor visitor = new BlackAndWhitelistVisitor(this.operatorPlan);
        visitor.visit();
    }

    private class BlackAndWhitelistVisitor extends LogicalRelationalNodesVisitor {
        private static final int ERROR_CODE = 1855;

        private final Splitter splitter;
        private final Set<String> blacklist;
        private final Set<String> whitelist;

        protected BlackAndWhitelistVisitor(OperatorPlan plan) throws FrontendException {
            super(plan, new DepthFirstWalker(plan));
            blacklist = Sets.newHashSet();
            whitelist = Sets.newHashSet();
            splitter = Splitter.on(',').trimResults().omitEmptyStrings();

            init();
        }

        private void init() {
            String blacklistConfig = pigContext.getProperties().getProperty(PigConfiguration.PIG_BLACKLIST);
            // Set blacklist only if it's been defined by a user
            if (blacklistConfig != null) {
                Iterable<String> iter = splitter.split(blacklistConfig);
                for(String elem : iter) {
                    blacklist.add(elem.toLowerCase());
                }
            }

            String whitelistConfig = pigContext.getProperties().getProperty(PigConfiguration.PIG_WHITELIST);
            // Set whitelist only if it's been defined by a user
            if (whitelistConfig != null) {
                Iterable<String> iter = splitter.split(whitelistConfig);
                for(String elem : iter) {
                    String lElem = elem.toLowerCase();
                    if(blacklist.contains(lElem)) {
                        throw new IllegalStateException("Conflict between whitelist and blacklist. '"+elem+"' appears in both.");
                    }
                    whitelist.add(lElem);
                }
            }
        }

        private void check(String operator) throws FrontendException {
            // throw an exception if the operator is not defined in whitelist
            if(whitelist != null && whitelist.size() > 0 && !whitelist.contains(operator)) {
                throw new FrontendException(operator +" is disabled. ", ERROR_CODE);
            }

            // throw an exception if operator is defined in blacklist
            if(blacklist != null && blacklist.size() > 0 && blacklist.contains(operator)) {
                throw new FrontendException(operator + " is disabled. ", ERROR_CODE);
            }
        }
        
        @Override
        public void visit(LOLoad load) throws FrontendException {
            check("load");
        }

        @Override
        public void visit(LOFilter filter) throws FrontendException {
            check("filter");
        }

        @Override
        public void visit(LOStore store) throws FrontendException {
            check("store");
        }

        @Override
        public void visit(LOJoin join) throws FrontendException {
            check("join");
        }

        @Override
        public void visit(LOForEach foreach) throws FrontendException {
            check("foreach");
        }

        @Override
        public void visit(LOGenerate gen) throws FrontendException {
        }

        public void visit(LOInnerLoad load) throws FrontendException {
        }

        @Override
        public void visit(LOCube cube) throws FrontendException {
            check("cube");
        }

        public void visit(LOCogroup loCogroup) throws FrontendException {
            check("group");
            check("cogroup");
        }

        @Override
        public void visit(LOSplit loSplit) throws FrontendException {
            check("split");
        }

        @Override
        public void visit(LOSplitOutput loSplitOutput) throws FrontendException {
        }

        @Override
        public void visit(LOUnion loUnion) throws FrontendException {
            check("union");
        }

        @Override
        public void visit(LOSort loSort) throws FrontendException {
            check("order");
        }

        @Override
        public void visit(LORank loRank) throws FrontendException {
            check("rank");
        }

        @Override
        public void visit(LODistinct loDistinct) throws FrontendException {
            check("distinct");
        }

        @Override
        public void visit(LOLimit loLimit) throws FrontendException {
            check("limit");
        }

        @Override
        public void visit(LOCross loCross) throws FrontendException {
            check("cross");
        }

        @Override
        public void visit(LOStream loStream) throws FrontendException {
            check("stream");
        }

        @Override
        public void visit(LONative nativeMR) throws FrontendException {
            check("mapreduce");
        }

    }

}
