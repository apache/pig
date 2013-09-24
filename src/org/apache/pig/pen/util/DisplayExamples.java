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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.Operator;
import org.apache.pig.impl.util.IdentityHashSet;

//Class containing some generic printing methods to print example data in a simple/tabular form
public class DisplayExamples {

    //public static StringBuffer result = new StringBuffer();
    public static final int MAX_DATAATOM_LENGTH = 25;

    static void printMetrics(
            Operator op,
            Map<Operator, DataBag> derivedData,
            Map<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses) {
        /*
         * System.out.println("Realness : " + Metrics.getRealness(op,
         * derivedData, true)); System.out.println("Completeness : " +
         * Metrics.getCompleteness(op, derivedData, OperatorToEqClasses, true));
         * System.out.println("Consiseness : " + Metrics.getConciseness(op,
         * derivedData, OperatorToEqClasses, true));
         */
        System.out.println("Realness : "
                + MetricEvaluation.getRealness(op, derivedData, true)
                + "\n"
                + "Conciseness : "
                + MetricEvaluation.getConciseness(op, derivedData,
                        OperatorToEqClasses, true)
                + "\n"
                + "Completeness : "
                + MetricEvaluation.getCompleteness(op, derivedData,
                        OperatorToEqClasses, true) + "\n");
    }

    public static String printTabular(LogicalPlan lp,
            Map<Operator, DataBag> exampleData,
            Map<LOForEach, Map<LogicalRelationalOperator, DataBag>> forEachInnerLogToDataMap) throws FrontendException {
        StringBuffer output = new StringBuffer();
        Set<Operator> seen = new HashSet<Operator>();
        for (Operator currentOp : lp.getSinks()) {
            if (currentOp instanceof LOStore && ((LOStore) currentOp).isTmpStore())
            {
                // display the branches of the temporary store first
                printTabular(currentOp, lp, exampleData, forEachInnerLogToDataMap, seen, output);
            }
        }
        for (Operator currentOp : lp.getSinks())
            printTabular(currentOp, lp, exampleData, forEachInnerLogToDataMap, seen, output);
        return output.toString();
    }

    static void printTabular(Operator op,
            LogicalPlan lp,
            Map<Operator, DataBag> exampleData,
            Map<LOForEach, Map<LogicalRelationalOperator, DataBag>> forEachInnerLogToDataMap,
            Set<Operator> seen,
            StringBuffer output) {

        List<Operator> inputs = lp.getPredecessors(op);
        if (inputs != null) { // to avoid an exception when op == LOLoad
            for (Operator Op : inputs) {
                if (!seen.contains(Op))
                  printTabular(Op, lp, exampleData, forEachInnerLogToDataMap, seen, output);
            }
        }
        seen.add(op);
        // print inner block first
        if ((op instanceof LOForEach)) {
            printNestedTabular((LOForEach)op, forEachInnerLogToDataMap, exampleData.get(op), output);
        }
        
        if (((LogicalRelationalOperator)op).getAlias() != null) {
            DataBag bag = exampleData.get(op);
            if (op instanceof LOLoad && ((LOLoad)op).getCastState()==LOLoad.CastState.INSERTED)
            {
                op = op.getPlan().getSuccessors(op).get(0);
                bag = exampleData.get(op);
            }
            try {
                DisplayTable(MakeArray(op, bag), op, bag, output);
            } catch (FrontendException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    // print out nested gen block in ForEach
    static void printNestedTabular(LOForEach foreach,
            Map<LOForEach, Map<LogicalRelationalOperator, DataBag>> forEachInnerLogToDataMap,
            DataBag foreachData,
            StringBuffer output) {
        LogicalPlan plan = foreach.getInnerPlan();
        if (plan != null) {
            printNestedTabular(plan.getSinks().get(0), plan, foreach.getAlias(), foreachData, forEachInnerLogToDataMap.get(foreach), output);
        }
    }

    static void printNestedTabular(Operator lo, LogicalPlan lp, String foreachAlias, DataBag foreachData, 
            Map<LogicalRelationalOperator, DataBag> logToDataMap, StringBuffer output) {
        
        List<Operator> inputs = lp.getPredecessors(lo);
        if (inputs != null) {
            for (Operator op : inputs)
                printNestedTabular(op, lp, foreachAlias, foreachData, logToDataMap, output);
        }
        
        DataBag bag = logToDataMap.get(lo);
        if (bag == null)
          return;
        
        if (((LogicalRelationalOperator)lo).getAlias() != null) {
            try {
              DisplayNestedTable(MakeArray(lo, bag), lo, foreachAlias, foreachData, bag, output);
            } catch (FrontendException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            } catch (Exception e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
        }
    }
    
    public static void printSimple(Operator op, LogicalPlan lp,
            Map<Operator, DataBag> exampleData) {
        DataBag bag = exampleData.get(op);

        List<Operator> inputs = lp.getPredecessors(op);
        if (inputs != null) {
            for (Operator lOp : inputs) {
                printSimple(lOp, lp, exampleData);
            }
        }
        if (((LogicalRelationalOperator)op).getAlias() != null) {
            // printTable(op, bag, output);
            // DisplayTable(MakeArray(op, bag), op, bag, output);
            System.out.println(((LogicalRelationalOperator)op).getAlias() + " : " + bag);
        }
        // System.out.println(op.getAlias() + " : " + bag);
    }

    static String AddSpaces(int n, boolean printSpace) {
        StringBuffer str = new StringBuffer();
        for (int i = 0; i < n; ++i) {
            if (printSpace)
                str.append(" ");
            else
                str.append("-");
        }
        return str.toString();
    }

    static void DisplayTable(String[][] table, Operator op, DataBag bag,
            StringBuffer output) throws FrontendException {
        if (op instanceof LOStore && ((LOStore) op).isTmpStore())
            return;
        
        int cols = ((LogicalRelationalOperator)op).getSchema().getFields().size();
        List<LogicalSchema.LogicalFieldSchema> fields = ((LogicalRelationalOperator)op).getSchema().getFields();
        int rows = (int) bag.size();
        int[] maxColSizes = new int[cols];
        for (int i = 0; i < cols; ++i) {
            maxColSizes[i] = fields.get(i).toString().length();
            if (maxColSizes[i] < 5)
                maxColSizes[i] = 5;
        }
        int total = 0;
        final String alias = ((LogicalRelationalOperator)op).getAlias();
        int aliasLength = (op instanceof LOStore ? alias.length() + 12 : alias.length() + 4);
        for (int j = 0; j < cols; ++j) {
            for (int i = 0; i < rows; ++i) {
                int length = table[i][j].length();
                if (length > maxColSizes[j])
                    maxColSizes[j] = length;
            }
            total += maxColSizes[j];
        }

        // Note of limit reset
        if (op instanceof LOLimit) {
            output.append("\nThe limit now in use, " + ((LOLimit)op).getLimit() + ", may have been changed for ILLUSTRATE purpose.\n");
        }
        
        // Display the schema first
        output
                .append(AddSpaces(total + 3 * (cols + 1) + aliasLength + 1,
                        false)
                        + "\n");
        if (op instanceof LOStore)
            output.append("| Store : " + alias + AddSpaces(4, true) + " | ");
        else
            output.append("| " + alias + AddSpaces(4, true) + " | ");
        for (int i = 0; i < cols; ++i) {
            String field = fields.get(i).toString(false);
            output.append(field
                    + AddSpaces(maxColSizes[i] - field.length(), true) + " | ");
        }
        output.append("\n"
                + AddSpaces(total + 3 * (cols + 1) + aliasLength + 1, false)
                + "\n");
        // now start displaying the data
        for (int i = 0; i < rows; ++i) {
            output.append("| " + AddSpaces(aliasLength, true) + " | ");
            for (int j = 0; j < cols; ++j) {
                String str = table[i][j];
                output.append(str
                        + AddSpaces(maxColSizes[j] - str.length(), true)
                        + " | ");
            }
            output.append("\n");
        }
        // now display the finish line
        output
                .append(AddSpaces(total + 3 * (cols + 1) + aliasLength + 1,
                        false)
                        + "\n");
    }

    static void DisplayNestedTable(String[][] table, Operator op, String foreachAlias, DataBag bag,
            DataBag foreachData, StringBuffer output) throws FrontendException {
        LogicalRelationalOperator lop = (LogicalRelationalOperator) op;
        int cols = lop.getSchema().getFields().size();
        List<LogicalSchema.LogicalFieldSchema> fields = lop.getSchema().getFields();
        int rows = (int) bag.size();
        int[] maxColSizes = new int[cols];
        for (int i = 0; i < cols; ++i) {
            maxColSizes[i] = fields.get(i).toString().length();
            if (maxColSizes[i] < 5)
                maxColSizes[i] = 5;
        }
        int total = 0;
        final String alias = ((LogicalRelationalOperator)op).getAlias();
        int aliasLength = alias.length() + +foreachAlias.length() + 5;
        for (int j = 0; j < cols; ++j) {
            for (int i = 0; i < rows; ++i) {
                int length = table[i][j].length();
                if (length > maxColSizes[j])
                    maxColSizes[j] = length;
            }
            total += maxColSizes[j];
        }

        // Display the schema first
        output
                .append(AddSpaces(total + 3 * (cols + 1) + aliasLength + 1,
                        false)
                        + "\n");
        output.append("| " + foreachAlias + "." + alias + AddSpaces(4, true) + " | ");
        for (int i = 0; i < cols; ++i) {
            String field;
            field = fields.get(i).toString(false);
            output.append(field
                    + AddSpaces(maxColSizes[i] - field.length(), true) + " | ");
        }
        output.append("\n"
                + AddSpaces(total + 3 * (cols + 1) + aliasLength + 1, false)
                + "\n");
        // now start displaying the data
        for (int i = 0; i < rows; ++i) {
            output.append("| " + AddSpaces(aliasLength, true) + " | ");
            for (int j = 0; j < cols; ++j) {
                String str = table[i][j];
                output.append(str
                        + AddSpaces(maxColSizes[j] - str.length(), true)
                        + " | ");
            }
            output.append("\n");
        }
        // now display the finish line
        output
                .append(AddSpaces(total + 3 * (cols + 1) + aliasLength + 1,
                        false)
                        + "\n");
    }

    static String[][] MakeArray(Operator op, DataBag bag)
            throws Exception {
        int rows = (int) bag.size();
        int cols = ((LogicalRelationalOperator)op).getSchema().getFields().size();
        String[][] table = new String[rows][cols];
        Iterator<Tuple> it = bag.iterator();
        for (int i = 0; i < rows; ++i) {
            Tuple t = it.next();
            for (int j = 0; j < cols; ++j) {
                table[i][j] = ShortenField(t.get(j));
            }
        }
        return table;
    }

    static String ShortenField(Object d) throws ExecException {
        if (d instanceof Tuple)
            return ShortenField((Tuple) d);
        else if (d instanceof DataBag)
            return ShortenField((DataBag) d);
        else {
            // System.out.println("Unrecognized data-type received!!!");
            // return null;
            if (DataType.findTypeName(d) != null) {
                if (d == null)
                    return "";
                else
                    return d.toString();
            }
        }
        System.out.println("Unrecognized data-type received!!!");
        return null;
    }

    static String ShortenField(DataBag bag) throws ExecException {
        StringBuffer str = new StringBuffer();
        long size = bag.size();
        str.append("{");
        if (size > 3) {
            Iterator<Tuple> it = bag.iterator();
            str.append(ShortenField(it.next()));
            while (it.hasNext()) {
                Tuple t = it.next();
                if (!it.hasNext()) {
                    str.append(", ..., " + ShortenField(t));
                }
            }
        } else {
            for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
                Tuple t = it.next();
                if (it.hasNext()) {
                    str.append(ShortenField(t) + ", ");
                } else
                    str.append(ShortenField(t));
            }
        }
        str.append("}");
        return str.toString();
    }

    static String ShortenField(Tuple t) throws ExecException {
        StringBuffer str = new StringBuffer();
        int noFields = t.size();
        str.append("(");
        if (noFields > 3) {

            Object d = t.get(0);
            str.append(ShortenField(d) + ", ..., ");
            d = t.get(noFields - 1);

            str.append(ShortenField(d));

        } else {
            for (int i = 0; i < noFields; ++i) {
                Object d = t.get(i);

                if (i != (noFields - 1)) {
                    str.append(ShortenField(d) + ", ");
                } else {
                    str.append(ShortenField(d));
                }

            }
        }
        str.append(")");
        return str.toString();
    }

}
