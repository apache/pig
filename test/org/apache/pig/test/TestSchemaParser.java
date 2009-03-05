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

import org.junit.Test;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

import java.io.ByteArrayInputStream;

import junit.framework.TestCase;


public class TestSchemaParser extends TestCase {

    @Test
    public void test1() throws ParseException, FrontendException {
        String test = "fieldA: int, fieldB:double,fieldC:bYteaRRay, fieldD:charARRAY";
        Schema schema = parseSchema(test) ;
        assertEquals(schema.size(), 4) ;
        assertAliases(schema, "fieldA", "fieldB", "fieldC", "fieldD") ;
        assertTypes(schema, DataType.INTEGER, DataType.DOUBLE,
                            DataType.BYTEARRAY, DataType.CHARARRAY);
    }

    @Test
    public void test2() throws ParseException, FrontendException {
        String test = "fieldA: bag {tuple1:tuple(a:int,b:long,c:float,d:double)}";
        Schema schema = parseSchema(test) ;

        assertEquals(schema.size(), 1);
        assertEquals(schema.getField(0).type, DataType.BAG) ;
        assertEquals(schema.getField(0).alias, "fieldA") ;

        Schema inner = schema.getField(0).schema ;
        assertEquals(inner.size(), 1) ;
        assertAliases(inner.getField(0).schema, "a", "b", "c", "d") ;
        assertTypes(inner.getField(0).schema, DataType.INTEGER, DataType.LONG,
                            DataType.FLOAT, DataType.DOUBLE);

    }

   @Test
    public void test3() throws ParseException, FrontendException {
        String test = "tuple1: tuple(a:chararray,b:long),"
                    +" tuple2: tuple(c:int,d:float) ";
        Schema schema = parseSchema(test) ;

        assertEquals(schema.size(), 2);
        assertAliases(schema, "tuple1", "tuple2") ;
        assertTypes(schema, DataType.TUPLE, DataType.TUPLE);

        Schema inner1 = schema.getField(0).schema ;
        assertEquals(inner1.size(), 2);
        assertAliases(inner1, "a", "b") ;
        assertTypes(inner1, DataType.CHARARRAY, DataType.LONG);

        Schema inner2 = schema.getField(1).schema ;
        assertEquals(inner2.size(), 2);
        assertAliases(inner2, "c", "d") ;
        assertTypes(inner2, DataType.INTEGER, DataType.FLOAT) ;

    }

    @Test
    public void test4() throws ParseException, FrontendException {
        String test = "garage: bag{tuple1: tuple(num_tools: int)}, links: int";
        Schema schema = parseSchema(test) ;

        // the schema
        assertEquals(schema.size(), 2) ;
        assertAliases(schema, "garage", "links") ;
        assertTypes(schema, DataType.BAG, DataType.INTEGER);
        assertEquals(schema.getField(0).type, DataType.BAG) ;

        // inner schema
        Schema inner = schema.getField(0).schema ;
        assertEquals(inner.size(), 1) ;
        assertEquals(inner.getField(0).type, DataType.TUPLE) ;
        assertEquals(inner.getField(0).alias, "tuple1") ;

        // inner of inner
        Schema innerInner = inner.getField(0).schema ;
        assertEquals(innerInner.size(), 1) ;
        assertEquals(innerInner.getField(0).type, DataType.INTEGER) ;
    }

    private void assertAliases(Schema schema, String... aliases)
                                            throws FrontendException {
        for(int i=0; i < aliases.length;i++) {
            assertEquals(schema.getField(i).alias, aliases[i]);
        }
    }

    private void assertTypes(Schema schema, byte... types)
                                            throws FrontendException {
        for(int i=0; i < types.length;i++) {
            assertEquals(schema.getField(i).type, types[i]);
        }
    }

    private Schema parseSchema(String schemaString)
                                            throws ParseException {
        ByteArrayInputStream stream = new ByteArrayInputStream(schemaString.getBytes()) ;
        QueryParser parser = new QueryParser(stream) ;
        return parser.TupleSchema() ;
    }
}
