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
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.IdentityHashSet;

//Class containing some generic printing methods to print example data in a simple/tabular form
public class DisplayExamples {

    //public static StringBuffer result = new StringBuffer();
    public static final int MAX_DATAATOM_LENGTH = 25;

    static void printMetrics(
            LogicalOperator op,
            Map<LogicalOperator, DataBag> derivedData,
            Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OperatorToEqClasses) {
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
            Map<LogicalOperator, DataBag> exampleData,
            Map<LOForEach, Map<LogicalOperator, DataBag>> forEachInnerLogToDataMap) {
        StringBuffer output = new StringBuffer();
        Set<LogicalOperator> seen = new HashSet<LogicalOperator>();
        for (LogicalOperator currentOp : lp.getLeaves())
            printTabular(currentOp, exampleData, forEachInnerLogToDataMap, seen, output);
        return output.toString();
    }

    static void printTabular(LogicalOperator op,
            Map<LogicalOperator, DataBag> exampleData,
            Map<LOForEach, Map<LogicalOperator, DataBag>> forEachInnerLogToDataMap,
            Set<LogicalOperator> seen,
            StringBuffer output) {

        List<LogicalOperator> inputs = op.getPlan().getPredecessors(op);
        if (inputs != null) { // to avoid an exception when op == LOLoad
            for (LogicalOperator Op : inputs) {
                if (!seen.contains(Op))
                  printTabular(Op, exampleData, forEachInnerLogToDataMap, seen, output);
            }
        }
        seen.add(op);
        // print inner block first
        if ((op instanceof LOForEach)) {
            printNestedTabular((LOForEach)op, forEachInnerLogToDataMap, exampleData.get(op), output);
        }
        
        if (op.getAlias() != null) {
            DataBag bag = exampleData.get(op);
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
            Map<LOForEach, Map<LogicalOperator, DataBag>> forEachInnerLogToDataMap,
            DataBag foreachData,
            StringBuffer output) {
        List<LogicalPlan> plans = foreach.getForEachPlans();
        if (plans != null) {
            for (LogicalPlan plan : plans) {
                printNestedTabular(plan.getLeaves().get(0), foreach.getAlias(), foreachData, forEachInnerLogToDataMap.get(foreach), output);
            }
        }
    }

    static void printNestedTabular(LogicalOperator lo, String foreachAlias, DataBag foreachData, 
            Map<LogicalOperator, DataBag> logToDataMap, StringBuffer output) {
        
        List<LogicalOperator> inputs = lo.getPlan().getPredecessors(lo);
        if (inputs != null) {
            for (LogicalOperator op : inputs)
                printNestedTabular(op, foreachAlias, foreachData, logToDataMap, output);
        }
        
        DataBag bag = logToDataMap.get(lo);
        if (bag == null)
          return;
        
        if (lo.getAlias() != null) {
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
    
    public static void printSimple(LogicalOperator op,
            Map<LogicalOperator, DataBag> exampleData) {
        DataBag bag = exampleData.get(op);

        List<LogicalOperator> inputs = op.getPlan().getPredecessors(op);
        if (inputs != null) {
            for (LogicalOperator lOp : inputs) {
                printSimple(lOp, exampleData);
            }
        }
        if (op.getAlias() != null) {
            // printTable(op, bag, output);
            // DisplayTable(MakeArray(op, bag), op, bag, output);
            System.out.println(op.getAlias() + " : " + bag);
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

    static void DisplayTable(String[][] table, LogicalOperator op, DataBag bag,
            StringBuffer output) throws FrontendException {
        if (op instanceof LOStore && ((LOStore) op).isTmpStore())
            return;
        
        int cols = op.getSchema().getFields().size();
        List<FieldSchema> fields = op.getSchema().getFields();
        int rows = (int) bag.size();
        int[] maxColSizes = new int[cols];
        for (int i = 0; i < cols; ++i) {
            maxColSizes[i] = fields.get(i).toString().length();
            if (maxColSizes[i] < 5)
                maxColSizes[i] = 5;
        }
        int total = 0;
        int aliasLength = (op instanceof LOStore ? op.getAlias().length() + 12 : op.getAlias().length() + 4);
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
            output.append("| Store : " + op.getAlias() + AddSpaces(4, true) + " | ");
        else
            output.append("| " + op.getAlias() + AddSpaces(4, true) + " | ");
        for (int i = 0; i < cols; ++i) {
            String field = fields.get(i).toString();
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

    static void DisplayNestedTable(String[][] table, LogicalOperator op, String foreachAlias, DataBag bag,
            DataBag foreachData, StringBuffer output) throws FrontendException {
        int cols = op.getSchema().getFields().size();
        List<FieldSchema> fields = op.getSchema().getFields();
        int rows = (int) bag.size();
        int[] maxColSizes = new int[cols];
        for (int i = 0; i < cols; ++i) {
            maxColSizes[i] = fields.get(i).toString().length();
            if (maxColSizes[i] < 5)
                maxColSizes[i] = 5;
        }
        int total = 0;
        int aliasLength = op.getAlias().length() + +foreachAlias.length() + 5;
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
        output.append("| " + foreachAlias + "." + op.getAlias() + AddSpaces(4, true) + " | ");
        for (int i = 0; i < cols; ++i) {
            String field;
            field = fields.get(i).toString();
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

    static String[][] MakeArray(LogicalOperator op, DataBag bag)
            throws Exception {
        int rows = (int) bag.size();
        int cols = op.getSchema().getFields().size();
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
