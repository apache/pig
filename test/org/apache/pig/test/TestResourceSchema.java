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

import junit.framework.TestCase;

import org.apache.pig.experimental.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.test.utils.TypeCheckingTestUtil;
import org.junit.Test;

import junit.framework.TestCase;

public class TestResourceSchema extends TestCase {

    /**
     * Test that ResourceSchema is correctly created given a
     * pig.Schema and vice versa
     * @throws FrontendException 
     * @throws SchemaMergeException 
     * @throws ExecException 
     */
    @Test
    public void testResourceFlatSchemaCreation() throws ExecException, SchemaMergeException, FrontendException {
        String [] aliases ={"f1", "f2"};
        byte[] types = {DataType.CHARARRAY, DataType.INTEGER};
        Schema origSchema = TypeCheckingTestUtil.genFlatSchema(
                aliases,types);
        ResourceSchema rsSchema = new ResourceSchema(origSchema);
        assertEquals("num fields", aliases.length, rsSchema.getFields().length);
        ResourceSchema.ResourceFieldSchema[] fields = rsSchema.getFields();
        for (int i=0; i<fields.length; i++) {
            assertEquals(fields[i].getName(), aliases[i]);
            assertEquals(fields[i].getType(), types[i]);
        }
        Schema genSchema = new Schema();
        for (ResourceSchema.ResourceFieldSchema field : rsSchema.getFields()) {
            FieldSchema pigFieldSchema;
            pigFieldSchema = DataType.determineFieldSchema(field);
            
            // determineFieldSchema only sets the types. we also want the aliases.
            // TODO this doesn't work properly for complex types 
            // (hence the "true" for relaxInner below)
            pigFieldSchema.alias = field.getName();
            
            genSchema.add(pigFieldSchema);
        } 
        assertTrue("generated schema equals original" , Schema.equals(genSchema, origSchema, true, false));
    }
    

}
