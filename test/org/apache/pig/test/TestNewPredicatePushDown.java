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
package org.apache.pig.test;

import org.apache.pig.*;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PredicatePushDownFilterExtractor;
import org.apache.pig.newplan.logical.expression.*;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.apache.pig.Expression.OpType.OP_NOT;
import static org.apache.pig.Expression.OpType.OP_NULL;
import static org.apache.pig.Expression.OpType.OP_AND;
import static org.apache.pig.Expression.OpType.OP_EQ;

/**
 * unit tests to test extracting new push-down filter conditions out of the filter
 * condition in the filter following a load which talks to metadata system (.i.e.
 * implements {@link LoadMetadata})
 */
public class TestNewPredicatePushDown {
    static PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    String query = "a = load 'foo' as (srcid:int, mrkt:chararray, dstid:int, name:chararray, " +
            "age:int, browser:map[], location:tuple(country:chararray, zip:int));";

    // PIG-4940
    @Test
    public void testSingleUnaryExpressionUnsupportedPushdown() throws Exception {
        String q = query + "b = filter a by not browser#'type' is null;" +
                "store b into 'out';";
        test(q, Arrays.asList("browser"), Arrays.asList(OP_NOT, OP_NULL), null,
                "(not (browser#'type' is null))", true);
    }

    // PIG-4953
    @Test
    public void testSingleUnaryExpressionSuccessfulPushdown() throws Exception {
        String q = query + "b = filter a by mrkt is not null;" +
                "store b into 'out';";
        test(q, Arrays.asList("mrkt"), Arrays.asList(OP_NOT, OP_NULL),
                "((mrkt is null) not)", null, true);
    }

    @Test
    public void testUnaryAndExpressionSuccessfulPushdown() throws Exception {
        String q = query + "b = filter a by not (mrkt is null and mrkt == 'us');" +
                "store b into 'out';";
        test(q, Arrays.asList("mrkt"), Arrays.asList(OP_NOT, OP_NULL, OP_AND, OP_EQ),
                "(((mrkt is null) and (mrkt == 'us')) not)", null, true);
    }

    @Test
    public void testUnaryAndExpressionUnsupportedPushdown() throws Exception {
        String q = query + "b = filter a by not (mrkt is null and mrkt != 'us');" +
                "store b into 'out';";
        test(q, Arrays.asList("mrkt"), Arrays.asList(OP_NOT, OP_NULL, OP_AND, OP_EQ),
                null, "(not ((mrkt is null) and (mrkt != us)))", true);
    }

    private PredicatePushDownFilterExtractor test(String query, List<String> predicateCols, List<Expression.OpType> supportedOpTypes,
                                                  String expPushFilterString, String expFilterString, boolean unsupportedExpression)
            throws Exception {
        PigServer pigServer = new PigServer( pc );
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        Operator op = newLogicalPlan.getSinks().get(0);
        LOFilter filter = (LOFilter)newLogicalPlan.getPredecessors(op).get(0);
        PredicatePushDownFilterExtractor pushColExtractor = new PredicatePushDownFilterExtractor(
                filter.getFilterPlan(), predicateCols, supportedOpTypes);
        pushColExtractor.visit();

        if (expPushFilterString == null) {
            Assert.assertEquals("Checking partition column filter:", null,
                    pushColExtractor.getPushDownExpression());
        } else  {
            Assert.assertEquals("Checking partition column filter:",
                    expPushFilterString,
                    pushColExtractor.getPushDownExpression().toString());
        }

        if (expFilterString == null) {
            Assert.assertTrue("Check that filter can be removed:",
                    pushColExtractor.isFilterRemovable());
        } else {
            if (unsupportedExpression) {
                String actual = TestNewPartitionFilterPushDown.getTestExpression(
                        (LogicalExpression)pushColExtractor.getFilteredPlan().getSources().get(0)).toString();
                Assert.assertEquals("checking trimmed filter expression:", expFilterString, actual);
            } else {
                String actual = pushColExtractor.getExpression(
                        (LogicalExpression)pushColExtractor.getFilteredPlan().getSources().get(0)).toString();
                Assert.assertEquals("checking trimmed filter expression:", expFilterString, actual);
            }
        }
        return pushColExtractor;
    }

}
