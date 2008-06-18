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
package org.apache.pig.impl.logicalLayer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;

public class LogicalPlan extends OperatorPlan<LogicalOperator> {
    private static final long serialVersionUID = 2L;

    public LogicalPlan() {
        super();
    }
    public LogicalOperator getSingleLeafPlanOutputOp()  {
        List<LogicalOperator> list = this.getLeaves() ;
        if (list.size() != 1) {
            throw new AssertionError("The plan has more than one leaf node") ;
        }
        return list.get(0) ;
    }

    public byte getSingleLeafPlanOutputType()  {
        return getSingleLeafPlanOutputOp().getType() ;
    }

    public void explain(OutputStream out, PrintStream ps){
        LOPrinter lpp = new LOPrinter(ps, this);

        try {
            lpp.print(out);
        } catch (VisitorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
