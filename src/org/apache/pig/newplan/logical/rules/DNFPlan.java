/**
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


package org.apache.pig.newplan.logical.rules;

import java.io.PrintStream;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.expression.*;
import org.apache.pig.newplan.Operator;

/**
 * A plan for DNF boolean tree so the size can be controlled.
 *
 */
class DNFPlan extends LogicalExpressionPlan {
    private static final int sizeLimit = 100;

    @Override
    public void explain(PrintStream ps, String format, boolean verbose)
                    throws FrontendException {
        ps.println("#-----------------------------------------------");
        ps.println("# DNF Plan:");
        ps.println("#-----------------------------------------------");
    }

    public void safeAdd(Operator op) throws FrontendException {
        // addition after size limit check
        if (size() + 1 > sizeLimit)
            throw new FrontendException(
                            "DNF size limit of " + sizeLimit + " is reached");
        super.add(op);
    }
}
