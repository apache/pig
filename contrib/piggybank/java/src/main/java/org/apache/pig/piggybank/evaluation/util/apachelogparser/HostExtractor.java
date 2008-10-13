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

/*
 * HostExtractor takes a url and returns the host. For example,
 * 
 * http://sports.espn.go.com/mlb/recap?gameId=281009122
 * 
 * leads to
 * 
 * sports.espn.go.com
 * 
 * Pig latin usage looks like
 * 
 * host = FOREACH row GENERATE
 * org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(referer);
 */

package org.apache.pig.piggybank.evaluation.util.apachelogparser;


import java.net.URL;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;

public class HostExtractor extends EvalFunc<DataAtom> {
    @Override
    public void exec(Tuple input, DataAtom output) {
        String string = input.getAtomField(0).strval();

        if (string == null)
            return;

        String host = null;
        try {
            host = new URL(string).getHost().toLowerCase();
        } catch (Exception e) {
        }
        if (host != null)
            output.setValue(host);
    }
}
