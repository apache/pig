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

package org.apache.hadoop.owl.logical;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.junit.Test;

public class TestSchemaUtil {

    class ValidityTestCase {
        String schema;
        boolean intendedToSucceed;

        public ValidityTestCase(String schema, boolean intendedToSucceed){
            this.schema = schema;
            this.intendedToSucceed = intendedToSucceed;
        }

        public void testValidate() throws OwlException{
            try {
                SchemaUtil.validateSchema(schema);
                if (! intendedToSucceed){
                    // test case was supposed to fail, this is an error.
                    fail("Validate negative testcase ["+schema+"] validated. This is a failure");
                }else{
                    System.out.println("Validate positive testcase ["+schema+"] validated. This is correct behaviour.");
                }
            } catch (OwlException e){
                if (intendedToSucceed){
                    // test case was supposed to succeed, this is an error.
                    fail("Validate positive testcase ["+schema+"] failed validation. This is a failure");
                }else{
                    // test case was supposed to fail, this is good.
                    if ((e.getErrorType() != ErrorType.ZEBRA_SCHEMA_EXCEPTION) && 
                            (e.getErrorType() != ErrorType.ZEBRA_TOKENMRGERROR_EXCEPTION)){
                        throw e; // rethrow exception, we don't know what this failure was, we let junit error on it.
                    }else{
                        System.out.println(
                                "Validate negative testcase [" + schema
                                + "] failed throwing [" + e.getMessage()
                                + ". This is correct behaviour."
                        );
                    }
                }
            }
        }
    };

    class ConsolidationTestCase {
        String baseSchema;
        String schemaToAppend;
        String expectedConsolidatedSchema;
        boolean intendedToSucceed;

        public ConsolidationTestCase(String baseSchema, String schemaToAppend, boolean intendedToSucceed, String expectedConsolidatedSchema){
            this.baseSchema = baseSchema;
            this.schemaToAppend = schemaToAppend;
            this.expectedConsolidatedSchema = expectedConsolidatedSchema;
            this.intendedToSucceed = intendedToSucceed;
        }

        public void testConsolidate() throws OwlException{
            try {
                String consolidatedSchema = SchemaUtil.consolidateSchema(baseSchema, schemaToAppend);
                if (! intendedToSucceed){
                    // test case was supposed to fail, this is an error.
                    fail(
                            "Consolidate negative testcase ["
                            + baseSchema + "] * [" + schemaToAppend
                            + "] did not throw an exception, and consolidated to ["
                            + consolidatedSchema + "]. This is a failure."
                    );
                }else{
                    if (consolidatedSchema.equals(expectedConsolidatedSchema)){
                        System.out.println(
                                "Consolidate positive testcase ["
                                + baseSchema+"] * ["+schemaToAppend
                                + "] => ["+expectedConsolidatedSchema+"] succeeded."
                        );
                    } else {
                        fail(
                                "Consolidate positive testcase ["
                                + baseSchema + "] * [" + schemaToAppend
                                + "] => [" + expectedConsolidatedSchema
                                + "] resulted in different consolidated result ["+consolidatedSchema+"]"
                        );
                    }
                }
            } catch (OwlException e){
                if (intendedToSucceed){
                    // test case was supposed to succeed, this is an error.
                    fail(
                            "Consolidate positive testcase ["
                            + baseSchema + "] * [" + schemaToAppend
                            + "] => ["+expectedConsolidatedSchema+"] failed consolidation. This is a failure"
                    );
                }else{
                    // test case was supposed to fail, this is good.
                    if ((e.getErrorType() != ErrorType.ZEBRA_SCHEMA_EXCEPTION) && 
                            (e.getErrorType() != ErrorType.ZEBRA_TOKENMRGERROR_EXCEPTION)){
                        throw e; // rethrow exception, we don't know what this failure was, we let junit error on it.
                    }else{
                        System.out.println(
                                "Consolidate negative testcase ["
                                + baseSchema + "] * [" + schemaToAppend 
                                + "] failed throwing [" + e.getMessage()
                                + "]. This is correct behaviour."
                        );
                    }
                }
            }
        }
    };


    /**
     * Test method for {@link org.apache.hadoop.owl.logical.SchemaUtil#validateSchema(java.lang.String)}.
     * @throws OwlException 
     */
    @Test
    public void testValidateSchema() throws OwlException {

        List<ValidityTestCase> schemaValidityTestCases = new ArrayList<ValidityTestCase>();

        // Trivial cases
        schemaValidityTestCases.add(new ValidityTestCase(null,true)); 
        // In reality, expected false, but understandable
        schemaValidityTestCases.add(new ValidityTestCase("",true)); 
        // In reality, expected false, but understandable
        schemaValidityTestCases.add(new ValidityTestCase("c1",true)); 
        // works out to "c1:bytes" if type is not specified

        // Basic types
        schemaValidityTestCases.add(new ValidityTestCase("c1:integer",false)); // "integer" is not a recognized type
        schemaValidityTestCases.add(new ValidityTestCase("c1:int",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:INT",true)); // capitalizing still works

        schemaValidityTestCases.add(new ValidityTestCase("c1:bool",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:long",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:float",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:double",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:string",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:bytes",true));

        // mixed form, specifying type for some columns, not for others(thus making them default to "bytes"
        schemaValidityTestCases.add(new ValidityTestCase("c1,c2:bool",true));

        // column name clashes
        schemaValidityTestCases.add(new ValidityTestCase("c1:bool,c1:bool",false));
        schemaValidityTestCases.add(new ValidityTestCase("c1:bool,c1:int",false));

        // Complex types : record
        schemaValidityTestCases.add(new ValidityTestCase("c1:record(a:int,b:string)",true));

        // Complex type : collection
        schemaValidityTestCases.add(new ValidityTestCase("c1:collection(r1:record(a:int,b:string))",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:collection(r1:record(a))",true)); // will default to a:bytes inside

        // testing sub-column name clash potential
        schemaValidityTestCases.add(new ValidityTestCase("c1:collection(r1:record(f1:int,f2:int)),c2:collection(r2:record(f1:int,f2:int))",true));

        // Complex types : map
        schemaValidityTestCases.add(new ValidityTestCase("c1:map(int)",true)); 
        // key type is always string and not specified
        schemaValidityTestCases.add(new ValidityTestCase("c1:map<int>",false));
        schemaValidityTestCases.add(new ValidityTestCase("c1:map(string,int)",false));

        // Complex type combinatorics
        schemaValidityTestCases.add(new ValidityTestCase(
                "c1:collection(r1:record(a:int,b:int)),b:map(collection(r2:record(a:int,b:int)))",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:map(map(map(map(string))))",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:record(a:record(a:record(a:record(a:string))))",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:record(record(record(record(a:string))))",false));
        schemaValidityTestCases.add(new ValidityTestCase("c1:collection(r1:record(a:collection(r2:record(a:collection(r3:record(a:collection(r4:record(a:string))))))))",true));
        // test sub schema as a RECORD type within COLLECTION without a column name
        schemaValidityTestCases.add(new ValidityTestCase("c1:collection(record(a:collection(record(a:collection(record(a:collection(record(a:string))))))))",true));
        schemaValidityTestCases.add(new ValidityTestCase("c1:collection(record(record(record(a:string))))",false));
        schemaValidityTestCases.add(new ValidityTestCase("c1:collection(record(r1:record(r2:record(a:string))))",true));


        for (ValidityTestCase testcase : schemaValidityTestCases){
            testcase.testValidate();
        }
    }

    /**
     * Test method for {@link org.apache.hadoop.owl.logical.SchemaUtil#consolidateSchema(java.lang.String, java.lang.String)}.
     * @throws OwlException 
     */
    @Test
    public void testConsolidateSchema() throws OwlException {
        List<ConsolidationTestCase> schemaConsolidationTestCases = new ArrayList<ConsolidationTestCase>();

        // Basic trivial cases
        schemaConsolidationTestCases.add(new ConsolidationTestCase("blah","yikes",true,"blah:bytes,yikes:bytes"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("blah",null,true,"blah"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("blah","",true,"blah:bytes"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("","yikes",true,"yikes:bytes"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase(null,"yikes",true,"yikes"));

        // Consolidation cases
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:int","c2:string",true,"c1:int,c2:string"));

        // Same column-names
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:int","c1:string",false,null));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:int,c2:string","c2:int",false,null));

        // Consolidation with subset 
        // User case: suppose partition has fewer columns, no change to table level schema
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:int,c2:string","c2:string",true,"c1:int,c2:string"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:int,c2:int","c2:int",true,"c1:int,c2:int"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:int,c2:int","",true,"c1:int,c2:int"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:int,c2:int",null,true,"c1:int,c2:int"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:int,c2:int","c2:int,c3:int",true,"c1:int,c2:int,c3:int"));

        // Consolidation with subset interesting cases because of missing type defaulting to bytes
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1,c2:int","c2",false,null));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1,c2:int","c1:int",false,null));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1,c2:int","c2:int",true,"c1:bytes,c2:int"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1,c2:int","",true,"c1:bytes,c2:int"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1,c2:int",null,true,"c1,c2:int"));

        // Consolidation inside complex columns
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:collection(r1:record(f1:int,f2:int))","c1:collection(r1:record(f1:int,f2:int,f3:int))",false,null)); 
        // Note : This was an interesting case - actually expected "c1:collection(f1:int,f2:int,f3:int)"
        // but schema consolidation does not happen inside complex nested types. Equality is checked for the purpose 
        // of top-level consolidation, but that's the extent of it.

        // Case-sensitivity
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:int,c2:string","C2:string",true,"c1:int,c2:string,C2:string"));
        schemaConsolidationTestCases.add(new ConsolidationTestCase("c1:int,c2:string","c2:STRING",true,"c1:int,c2:string"));


        for (ConsolidationTestCase testcase : schemaConsolidationTestCases){
            testcase.testConsolidate();
        }

    }

}
