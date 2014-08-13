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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.regex;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

public class CompiledAutomaton implements RegexImpl {

    private RunAutomaton runauto = null;
    
    public CompiledAutomaton( String rhsPattern ) {
        RegExp regexpr = new dk.brics.automaton.RegExp(rhsPattern, RegExp.NONE);
        Automaton auto = regexpr.toAutomaton();
        this.runauto = new RunAutomaton(auto, true);
    }
    
    @Override
    public boolean match(String lhs, String rhs) {
        return this.runauto.run(lhs);
    }

}
