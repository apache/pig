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
package org.apache.pig.penny.apps.gl.goldenLogicClasses;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.penny.apps.gl.GoldenLogic;


public class FlattenLinksGL implements GoldenLogic {

    public Collection<Tuple> run(Tuple t) throws ExecException {
        Collection<Tuple> out = new LinkedList<Tuple>();
        Object domain = t.get(1);
        DataBag inlinks = (DataBag) t.get(9);
        if (inlinks == null) {
            Tuple outT = new DefaultTuple();
            outT.append(domain);
            outT.append(null);
            out.add(outT);
        } else if (inlinks.size() == 0) {            // not sure if this is correct or not -- in any case our data doesn't have any empty sets -- only sets with a single empty tuple
            Tuple outT = new DefaultTuple();
            outT.append(domain);
            out.add(outT);
        } else {
            for (Iterator<Tuple> it = inlinks.iterator(); it.hasNext(); ) {
                Tuple inlink = it.next();
                Tuple outT = new DefaultTuple();
                outT.append(domain);
                if (inlink != null) {
                    for (int i = 0; i < inlink.size(); i++) {
                        outT.append(inlink.get(i));
                    }
                }
                out.add(outT);
            }
        }
        return out;
    }

}
