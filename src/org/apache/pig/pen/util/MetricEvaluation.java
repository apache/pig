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

package org.apache.pig.pen.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.impl.util.IdentityHashSet;

//Evaluates various metrics
public class MetricEvaluation {
    public static float getRealness(Operator op,
            Map<Operator, DataBag> exampleData, boolean overallRealness) {
        // StringBuffer str = new StringBuffer();
        int noTuples = 0;
        int noSynthetic = 0;
        for (Map.Entry<Operator, DataBag> e : exampleData.entrySet()) {
            // if(e.getKey() instanceof LORead) continue;
            if (((LogicalRelationalOperator)e.getKey()).getAlias() == null)
                continue;
            DataBag bag;
            if (overallRealness) {
                bag = exampleData.get(e.getKey());
            } else {
                bag = exampleData.get(op);
            }
            noTuples += bag.size();
            for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
                if (((ExampleTuple) it.next()).synthetic)
                    noSynthetic++;
            }
            if (!overallRealness)
                break;

        }

        if (noTuples == 0) {
            if (noSynthetic == 0)
                return 0.0f;
            else
                return 100.0f;
        }
        return 100 * (1 - ((float) noSynthetic / (float) noTuples));

    }

    public static float getConciseness(
            Operator op,
            Map<Operator, DataBag> exampleData,
            Map<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses,
            boolean overallConciseness) {
        DataBag bag = exampleData.get(op);

        int noEqCl = OperatorToEqClasses.get(op).size();
        long noTuples = bag.size();

        float conciseness = 100 * ((float) noEqCl / (float) noTuples);
        if (!overallConciseness) {

            return ((conciseness > 100.0) ? 100.0f : conciseness);
        } else {

            noEqCl = 0;
            noTuples = 0;
            conciseness = 0;
            int noOperators = 0;

            for (Map.Entry<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> e : OperatorToEqClasses
                    .entrySet()) {
                if (e.getKey().getAlias() == null)
                    continue;
                noOperators++; // we need to keep a track of these and not use
                               // OperatorToEqClasses.size() as LORead shouldn't
                               // be considered a operator
                bag = exampleData.get(e.getKey());

                noTuples = bag.size();
                noEqCl = e.getValue().size();
                float concise = 100 * ((float) noEqCl / (float) noTuples);
                concise = (concise > 100) ? 100 : concise;
                conciseness += concise;
            }
            conciseness /= (float) noOperators;

            return conciseness;
        }

    }

    public static float getCompleteness(
            Operator op,
            Map<Operator, DataBag> exampleData,
            Map<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses,
            boolean overallCompleteness) {

        int noClasses = 0;
        int noCoveredClasses = 0;
        int noOperators = 0;
        float completeness = 0;
        if (!overallCompleteness) {
            Collection<IdentityHashSet<Tuple>> eqClasses = OperatorToEqClasses
                    .get(op);

            noCoveredClasses = getCompletenessLogic(eqClasses);
            noClasses = eqClasses.size();

            return 100 * ((float) noCoveredClasses) / (float) noClasses;
        } else {
            for (Map.Entry<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> e : OperatorToEqClasses
                    .entrySet()) {
                noCoveredClasses = 0;
                noClasses = 0;

                // if(e.getKey() instanceof LORead) continue; //We don't
                // consider LORead a operator.
                if (e.getKey().getAlias() == null)
                    continue; // we want to consider join a single operator
                noOperators++;
                Collection<IdentityHashSet<Tuple>> eqClasses = e.getValue();
                noCoveredClasses = getCompletenessLogic(eqClasses);
                noClasses += eqClasses.size();
                completeness += 100 * ((float) noCoveredClasses / (float) noClasses);
            }
            completeness /= (float) noOperators;

            return completeness;
        }

    }

    private static int getCompletenessLogic(
            Collection<IdentityHashSet<Tuple>> eqClasses) {
        int nCoveredClasses = 0;
        for (IdentityHashSet<Tuple> eqClass : eqClasses) {
            if (!eqClass.isEmpty())
                nCoveredClasses++;    
        }
        return nCoveredClasses;
    }
}
