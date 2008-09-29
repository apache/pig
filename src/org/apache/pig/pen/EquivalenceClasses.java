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

package org.apache.pig.pen;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.util.IdentityHashSet;


//These methods are used to generate equivalence classes given the operator name and the output from the operator
//For example, it gives out 2 eq. classes for filter, one that passes the filter and one that doesn't
public class EquivalenceClasses {
    public static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(
            LogicalOperator op, Map<LogicalOperator, DataBag> derivedData)
            throws ExecException {
        if (op instanceof LOCogroup)
            return GetEquivalenceClasses((LOCogroup) op, derivedData);
        else if (op instanceof LOForEach)
            return GetEquivalenceClasses((LOForEach) op, derivedData);
        else if (op instanceof LOFilter)
            return GetEquivalenceClasses((LOFilter) op, derivedData);
        else if (op instanceof LOSort)
            return GetEquivalenceClasses((LOSort) op, derivedData);
        else if (op instanceof LOSplit)
            return GetEquivalenceClasses((LOSplit) op, derivedData);
        else if (op instanceof LOUnion)
            return GetEquivalenceClasses((LOUnion) op, derivedData);
        else if (op instanceof LOLoad)
            return GetEquivalenceClasses((LOLoad) op, derivedData);
            throw new RuntimeException("Unrecognized logical operator.");
    }

    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LOLoad op,
            Map<LogicalOperator, DataBag> derivedData) {
        // Since its a load, all the tuples belong to a single equivalence class
        Collection<IdentityHashSet<Tuple>> equivClasses = new LinkedList<IdentityHashSet<Tuple>>();
        IdentityHashSet<Tuple> input = new IdentityHashSet<Tuple>();

        equivClasses.add(input);

        DataBag output = derivedData.get(op);

        for (Iterator<Tuple> it = output.iterator(); it.hasNext();) {
            Tuple t = it.next();

            input.add(t);
        }

        return equivClasses;
    }

    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(
            LOCogroup op, Map<LogicalOperator, DataBag> derivedData)
            throws ExecException {
        Collection<IdentityHashSet<Tuple>> equivClasses = new LinkedList<IdentityHashSet<Tuple>>();
        IdentityHashSet<Tuple> acceptableGroups = new IdentityHashSet<Tuple>();

        equivClasses.add(acceptableGroups);

        for (Iterator<Tuple> it = derivedData.get(op).iterator(); it.hasNext();) {
            Tuple t = it.next();

            boolean isAcceptable;

            if (t.size() == 2) {
                isAcceptable = (((DataBag) (t.get(1))).size() >= 2);
            } else {
                isAcceptable = true;
                for (int field = 1; field < t.size(); field++) {
                    DataBag bag = (DataBag) t.get(field);
                    if (bag.size() == 0) {
                        isAcceptable = false;
                        break;
                    }
                }
            }

            if (isAcceptable)
                acceptableGroups.add(t);

        }
        return equivClasses;
    }

    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(
            LOForEach op, Map<LogicalOperator, DataBag> derivedData) {
        Collection<IdentityHashSet<Tuple>> equivClasses = new LinkedList<IdentityHashSet<Tuple>>();

        IdentityHashSet<Tuple> equivClass = new IdentityHashSet<Tuple>();
        equivClasses.add(equivClass);

        for (Iterator<Tuple> it = derivedData.get(op).iterator(); it.hasNext();) {
            equivClass.add(it.next());
        }

        return equivClasses;
    }

    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(
            LOFilter op, Map<LogicalOperator, DataBag> derivedData) {
        Collection<IdentityHashSet<Tuple>> equivClasses = new LinkedList<IdentityHashSet<Tuple>>();

        IdentityHashSet<Tuple> pass = new IdentityHashSet<Tuple>();
        IdentityHashSet<Tuple> fail = new IdentityHashSet<Tuple>();

        for (Iterator<Tuple> it = derivedData.get(op).iterator(); it.hasNext();) {
            pass.add(it.next());
        }

        LogicalOperator input = op.getInput();

        for (Iterator<Tuple> it = derivedData.get(input).iterator(); it
                .hasNext();) {
            Tuple t = it.next();
            if (pass.contains(t))
                continue;
            else
                fail.add(t);
        }

        equivClasses.add(pass);
        equivClasses.add(fail);

        return equivClasses;

    }

    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LOSort op,
            Map<LogicalOperator, DataBag> derivedData) {
        //We don't create any eq. class for sort
        IdentityHashSet<Tuple> temp = new IdentityHashSet<Tuple>();
        Collection<IdentityHashSet<Tuple>> output = new LinkedList<IdentityHashSet<Tuple>>();
        output.add(temp);
        return output;
    }

    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LOSplit op,
            Map<LogicalOperator, DataBag> derivedData) {
        throw new RuntimeException(
                "LOSplit not supported yet in example generator.");
    }

    static Collection<IdentityHashSet<Tuple>> GetEquivalenceClasses(LOUnion op,
            Map<LogicalOperator, DataBag> derivedData) {

        // make one equivalence class per input relation

        Collection<IdentityHashSet<Tuple>> equivClasses = new LinkedList<IdentityHashSet<Tuple>>();

        for (LogicalOperator input : op.getInputs()) {
            IdentityHashSet<Tuple> equivClass = new IdentityHashSet<Tuple>();

            for (Iterator<Tuple> it = derivedData.get(input).iterator(); it
                    .hasNext();) {
                equivClass.add(it.next());
            }
            equivClasses.add(equivClass);
        }

        return equivClasses;
    }
}
