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

package org.apache.hadoop.owl.parser;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.logical.Command;
import org.apache.hadoop.owl.logical.CommandInfo.FilterString;
import org.apache.hadoop.owl.logical.CommandInfo.Operator;
import org.apache.hadoop.owl.parser.OwlParser;
import org.junit.Test;

public class TestOperator {

    Command getCommandFromDDL(String s) throws Exception{
        ByteArrayInputStream bi = new ByteArrayInputStream(s.getBytes());
        OwlParser parser = new OwlParser(bi);
        return parser.Start();
    }

    FilterString getFilterString(String op, String val) throws OwlException{
        return new FilterString(Operator.fromString(op.toUpperCase()),val);
    }

    /**
     *
     * Checks whether two values are equal strings, where one value, 
     * or the other might potentially have quotes trimmed.
     * @param input
     * @param potentiallyTrimmedOutput
     * @return true if both are equal with the exception of potentially having quotes trimmed
     */
    boolean checkEqualsWithPotentiallyTrimmedQuotes(String input, String potentiallyTrimmedOutput){
        if (input.equals(potentiallyTrimmedOutput)){
            return true;
        }

        if (input.equals("\"" + potentiallyTrimmedOutput + "\"")){
            return true;
        }

        return false;
    }

    @Test
    public void testOperator() throws Exception{

        Map<String,FilterString> filterMap = new HashMap<String,FilterString>();

        filterMap.put("eqkey", getFilterString("=", "\"xyz\""));
        filterMap.put("lekey", getFilterString("<=", "5"));
        filterMap.put("gekey", getFilterString(">=", "5"));
        filterMap.put("ltkey", getFilterString("<", "5"));
        filterMap.put("gtkey", getFilterString(">", "5"));
        filterMap.put("likeekey", getFilterString("LIKE", "\"%blah%\""));
        //        filterMap.put("intinkey", getFilterString("IN", "\"(3,4,5)\""));
        //        filterMap.put("strinkey", getFilterString("IN", "\"(\\\"three\\\",\\\"four\\\",\\\"five\\\")\""));

        String DDLStatement =
            "alter owltable foods "
            + "within owldatabase fooca "
            + "with partition "
            + "(";

        boolean firstkey = true;
        for (String s : filterMap.keySet()){
            FilterString fs = filterMap.get(s);
            if (firstkey){
                DDLStatement += " " + s + " " + fs.getFilterOperator().getOp() + " " + fs.getFilterValue() ;
                firstkey = false;
            } else {
                DDLStatement += " , " + s + " " + fs.getFilterOperator().getOp() + " " + fs.getFilterValue() ;
            }
        }

        DDLStatement += 
            " ) "
            + "modify property "
            + "( "
            + "colour = \"red\" "
            + ", shape = \"round\" "
            + ") ";

        System.out.println("Parsing["+DDLStatement+"]");
        Command alterCmd = getCommandFromDDL(DDLStatement);

        Map<String, FilterString> ptnFiltersExtractedFromCommand = alterCmd.getCommandInfo().getPartitionFilters();

        for (String s : filterMap.keySet()){
            FilterString fsi = filterMap.get(s);
            System.out.println("key["+s+"],i_op["+fsi.getFilterOperator()+"],i_value["+fsi.getFilterValue()+"]");
        }

        for (String s : ptnFiltersExtractedFromCommand.keySet()){
            FilterString fso = ptnFiltersExtractedFromCommand.get(s);
            System.out.println("key["+s+"],o_op["+fso.getFilterOperator()+"],o_value["+fso.getFilterValue()+"]");
        }

        assertEquals(filterMap.size(),ptnFiltersExtractedFromCommand.size());

        for (String s : ptnFiltersExtractedFromCommand.keySet()){
            FilterString fsi = filterMap.get(s);
            FilterString fso = ptnFiltersExtractedFromCommand.get(s);
            assertTrue(fso.getFilterOperator().equals(fsi.getFilterOperator()));
            assertTrue(checkEqualsWithPotentiallyTrimmedQuotes(fsi.getFilterValue(),fso.getFilterValue()));
        }
    }

}
