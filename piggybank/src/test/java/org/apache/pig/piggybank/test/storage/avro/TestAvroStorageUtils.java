/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.piggybank.test.storage.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.pig.piggybank.storage.avro.AvroStorageUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TestAvroStorageUtils {

    // Common elements of test records
    private final String TYPE_RECORD = "{ \"type\" : \"record\", ";
    private final String NAME_NODE   =   "\"name\": \"Node\" , ";
    private final String FIELDS_VALUE = " \"fields\": [ { \"name\": \"value\", \"type\":\"int\"}, ";

    public final String RECORD_BEGINNING = TYPE_RECORD + NAME_NODE + FIELDS_VALUE;

     @Test
     public void testGenericUnion() throws IOException {

        final String str1 = "[ \"string\", \"int\", \"boolean\"  ]";
        Schema s = Schema.parse(str1);
        assertTrue(AvroStorageUtils.containsGenericUnion(s));

        final String str2 = "[ \"string\", \"int\", \"null\"  ]";
        s = Schema.parse(str2);
        assertTrue(AvroStorageUtils.containsGenericUnion(s));

        final String str3 = "[ \"string\", \"null\"  ]";
        s = Schema.parse(str3);
        assertFalse(AvroStorageUtils.containsGenericUnion(s));
        Schema realSchema = AvroStorageUtils.getAcceptedType(s);
        assertEquals(AvroStorageUtils.StringSchema, realSchema);

        final String str4 =  "{\"type\": \"array\", \"items\": "  + str2 + "}";
        s = Schema.parse(str4);
        assertTrue(AvroStorageUtils.containsGenericUnion(s));
        try {
            realSchema = AvroStorageUtils.getAcceptedType(s);
            fail("\"Should throw a runtime exception when trying to get accepted type from a unacceptable union");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Cannot call this function on a unacceptable union"));
        }

        final String str5 = RECORD_BEGINNING +
                                 "{ \"name\": \"next\", \"type\": [\"null\", \"int\"] } ] }";
        s = Schema.parse(str5);
        assertFalse(AvroStorageUtils.containsGenericUnion(s));

        final String str6 = RECORD_BEGINNING +
                                 "{ \"name\": \"next\", \"type\": [\"string\", \"int\"] } ] }";
        s = Schema.parse(str6);
        assertTrue(AvroStorageUtils.containsGenericUnion(s));

        final String str7 = "[ \"string\"  ]"; /*union with one type*/
        s = Schema.parse(str7);
        assertFalse(AvroStorageUtils.containsGenericUnion(s));
        realSchema = AvroStorageUtils.getAcceptedType(s);
        assertEquals(AvroStorageUtils.StringSchema, realSchema);

        final String str8 = "[  ]"; /*union with no type*/
        s = Schema.parse(str8);
        assertFalse(AvroStorageUtils.containsGenericUnion(s));
        realSchema = AvroStorageUtils.getAcceptedType(s);
        assertNull(realSchema);
    }

    @Test
    public void testGetPaths() throws IOException {
        final String basedir = "file://" + System.getProperty("user.dir");
        final String tempdir = Long.toString(System.currentTimeMillis());
        final String nonexistentpath = basedir + "/" + tempdir + "/this_path_does_not_exist";

        String locationStr = null;
        Set<Path> paths;
        Configuration conf = new Configuration();

        // existent path
        locationStr = basedir;
        paths = AvroStorageUtils.getPaths(locationStr, conf, true);
        assertFalse(paths.isEmpty());

        // non-existent path
        locationStr = nonexistentpath;
        try {
            paths = AvroStorageUtils.getPaths(locationStr, conf, true);
            fail();
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("matches 0 files"));
        }

        // empty glob pattern
        locationStr = basedir + "/{}";
        try {
            paths = AvroStorageUtils.getPaths(locationStr, conf, true);
            fail();
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("matches 0 files"));
        }

        paths = AvroStorageUtils.getPaths(locationStr, conf, false);
        assertTrue(paths.isEmpty());

        // bad glob pattern
        locationStr = basedir + "/{1,";
        try {
            AvroStorageUtils.getPaths(locationStr, conf, true);
            Assert.fail("Negative test to test illegal file pattern. Should not be succeeding!");
        } catch (IOException e) {
            // The message of the exception for illegal file pattern is rather long,
            // so we simply confirm if it contains 'illegal file pattern'.
            assertTrue(e.getMessage().contains("Illegal file pattern"));
        }
    }

    // test merging null and non-null
    @Test
    public void testMergeSchema1() throws IOException {
        Schema nonNull = Schema.create(Schema.Type.INT);
        assertEquals(AvroStorageUtils.mergeSchema(null, null), null);
        assertEquals(AvroStorageUtils.mergeSchema(null, nonNull), nonNull);
        assertEquals(AvroStorageUtils.mergeSchema(nonNull, null), nonNull);
    }

    // test merging primitive types
    @Test
    public void testMergeSchema2() throws IOException {
        Schema.Type primitiveType[] = {
                Schema.Type.NULL,
                Schema.Type.BOOLEAN,
                Schema.Type.BYTES,
                Schema.Type.INT,
                Schema.Type.LONG,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                Schema.Type.ENUM,
                Schema.Type.STRING,
        };

        // First index is type1, and second index is type2. Note that Schema.Type.NULL
        // means the Avro null type while null means undefined.
        Schema.Type expectedType[][] = {
                {
                    Schema.Type.NULL,    // = null + null
                    null,                // = null + boolean
                    null,                // = null + bytes
                    null,                // = null + int
                    null,                // = null + long
                    null,                // = null + float
                    null,                // = null + double
                    null,                // = null + enum
                    null,                // = null + string
                },
                {
                    null,                // = boolean + null
                    Schema.Type.BOOLEAN, // = boolean + boolean
                    null,                // = boolean + bytes
                    null,                // = boolean + int
                    null,                // = boolean + long
                    null,                // = boolean + float
                    null,                // = boolean + double
                    null,                // = boolean + enum
                    null,                // = boolean + string
                },
                {
                    null,                // = bytes + null
                    null,                // = bytes + boolean
                    Schema.Type.BYTES,   // = bytes + bytes
                    null,                // = bytes + int
                    null,                // = bytes + long
                    null,                // = bytes + float
                    null,                // = bytes + double
                    null,                // = bytes + enum
                    null,                // = bytes + string
                },
                {
                    null,                // = int + null
                    null,                // = int + boolean
                    null,                // = int + bytes
                    Schema.Type.INT,     // = int + int
                    Schema.Type.LONG,    // = int + long
                    Schema.Type.FLOAT,   // = int + float
                    Schema.Type.DOUBLE,  // = int + double
                    Schema.Type.STRING,  // = int + enum
                    Schema.Type.STRING,  // = int + string
                },
                {
                    null,                // = long + null
                    null,                // = long + boolean
                    null,                // = long + bytes
                    Schema.Type.LONG,    // = long + int
                    Schema.Type.LONG,    // = long + long
                    Schema.Type.FLOAT,   // = long + float
                    Schema.Type.DOUBLE,  // = long + double
                    Schema.Type.STRING,  // = long + enum
                    Schema.Type.STRING,  // = long + string
                },
                {
                    null,                // = float + null
                    null,                // = float + boolean
                    null,                // = float + bytes
                    Schema.Type.FLOAT,   // = float + int
                    Schema.Type.FLOAT,   // = float + long
                    Schema.Type.FLOAT,   // = float + float
                    Schema.Type.DOUBLE,  // = float + double
                    Schema.Type.STRING,  // = float + enum
                    Schema.Type.STRING,  // = float + string
                },
                {
                    null,                // = double + null
                    null,                // = double + boolean
                    null,                // = double + bytes
                    Schema.Type.DOUBLE,  // = double + int
                    Schema.Type.DOUBLE,  // = double + long
                    Schema.Type.DOUBLE,  // = double + float
                    Schema.Type.DOUBLE,  // = double + double
                    Schema.Type.STRING,  // = double + enum
                    Schema.Type.STRING,  // = double + string
                },
                {
                    null,                // = enum + null
                    null,                // = enum + boolean
                    null,                // = enum + bytes
                    Schema.Type.STRING,  // = enum + int
                    Schema.Type.STRING,  // = enum + long
                    Schema.Type.STRING,  // = enum + float
                    Schema.Type.STRING,  // = enum + double
                    Schema.Type.ENUM,    // = enum + enum
                    Schema.Type.STRING,  // = enum + string
                },
                {
                    null,                // = string + null
                    null,                // = string + boolean
                    null,                // = string + bytes
                    Schema.Type.STRING,  // = string + int
                    Schema.Type.STRING,  // = string + long
                    Schema.Type.STRING,  // = string + float
                    Schema.Type.STRING,  // = string + double
                    Schema.Type.STRING,  // = string + enum
                    Schema.Type.STRING,  // = string + string
                },
        };

        List<String> enumSymbols = new ArrayList<String>();
        enumSymbols.add("sym1");
        enumSymbols.add("sym2");
        Schema enumSchema = Schema.createEnum("enum", null, null, enumSymbols);

        for (int i = 0; i < primitiveType.length; i++) {
            Schema.Type x = primitiveType[i];
            for (int j = 0; j < primitiveType.length; j++) {
                Schema.Type y = primitiveType[j];
                if (expectedType[i][j] == null) {
                    try {
                        Schema z = AvroStorageUtils.mergeSchema(
                                x.equals(Schema.Type.ENUM) ? enumSchema : Schema.create(x),
                                y.equals(Schema.Type.ENUM) ? enumSchema : Schema.create(y));
                        Assert.fail("exception is expected, but " + z.getType() + " is returned");
                    } catch (IOException e) {
                        assertEquals("Cannot merge "+ x +" with " +y, e.getMessage());
                    }
                } else {
                    Schema z = AvroStorageUtils.mergeSchema(
                            x.equals(Schema.Type.ENUM) ? enumSchema : Schema.create(x),
                            y.equals(Schema.Type.ENUM) ? enumSchema : Schema.create(y));
                    assertEquals(expectedType[i][j], z.getType());
                }
            }
        }
    }

    // test merging different kinds of complex types (negative test)
    // different kinds of complex types cannot be merged
    @Test
    public void testMergeSchema3() throws IOException {
        Schema complexType[] = {
            Schema.createRecord(new ArrayList<Schema.Field>()),
            Schema.createArray(Schema.create(Schema.Type.INT)),
            Schema.createMap(Schema.create(Schema.Type.INT)),
            Schema.createUnion(new ArrayList<Schema>()),
            Schema.createFixed("fixed", null, null, 1),
        };

        for (int i = 0; i < complexType.length; i++) {
            Schema x = complexType[i];
            for (int j = 0; j < complexType.length; j++) {
                Schema y = complexType[j];
                if (i != j) {
                    try {
                        Schema z = AvroStorageUtils.mergeSchema(x, y);
                        Assert.fail("exception is expected, but " + z.getType() + " is returned");
                    } catch (IOException e) {
                        assertEquals("Cannot merge "+ x.getType()+ " with "+ y.getType(), e.getMessage());
                    }
                }
            }
        }
    }

    // test merging two records
    @Test
    public void testMergeSchema4() throws IOException {
        Schema x, y, z;

        // fields have the same names and types
        List<Schema.Field> fields1 = new ArrayList<Schema.Field>();
        fields1.add(new Schema.Field("f1", Schema.create(Schema.Type.INT), null, null));
        x = Schema.createRecord(fields1);

        List<Schema.Field> fields2 = new ArrayList<Schema.Field>();
        fields2.add(new Schema.Field("f1", Schema.create(Schema.Type.INT), null, null));
        y = Schema.createRecord(fields2);

        z = AvroStorageUtils.mergeSchema(x, y);
        assertEquals(x.getFields().size(), z.getFields().size());
        assertEquals(Schema.Type.INT, z.getField("f1").schema().getType());

        // fields have the same names and mergeable types
        List<Schema.Field> fields3 = new ArrayList<Schema.Field>();
        fields3.add(new Schema.Field("f1", Schema.create(Schema.Type.INT), null, null));
        x = Schema.createRecord(fields3);

        List<Schema.Field> fields4 = new ArrayList<Schema.Field>();
        fields4.add(new Schema.Field("f1", Schema.create(Schema.Type.DOUBLE), null, null));
        y = Schema.createRecord(fields4);

        z = AvroStorageUtils.mergeSchema(x, y);
        assertEquals(x.getFields().size(), z.getFields().size());
        assertEquals(Schema.Type.DOUBLE, z.getField("f1").schema().getType());

        // fields have the same names but not mergeable types
        List<Schema.Field> fields5 = new ArrayList<Schema.Field>();
        fields5.add(new Schema.Field("f1", Schema.create(Schema.Type.INT), null, null));
        x = Schema.createRecord(fields5);

        List<Schema.Field> fields6 = new ArrayList<Schema.Field>();
        fields6.add(new Schema.Field("f1", Schema.create(Schema.Type.BOOLEAN), null, null));
        y = Schema.createRecord(fields6);

        try {
            z = AvroStorageUtils.mergeSchema(x, y);
            Assert.fail("exception is expected, but " + z.getType() + " is returned");
        } catch (IOException e) {
            assertEquals("Cannot merge "+ x.getField("f1").schema().getType() +
                         " with "+ y.getField("f1").schema().getType(), e.getMessage());
        }

        // fields have different names
        List<Schema.Field> fields7 = new ArrayList<Schema.Field>();
        fields7.add(new Schema.Field("f1", Schema.create(Schema.Type.INT), null, null));
        x = Schema.createRecord(fields7);

        List<Schema.Field> fields8 = new ArrayList<Schema.Field>();
        fields8.add(new Schema.Field("f2", Schema.create(Schema.Type.DOUBLE), null, null));
        y = Schema.createRecord(fields8);

        z = AvroStorageUtils.mergeSchema(x, y);
        assertEquals(x.getFields().size() + y.getFields().size(), z.getFields().size());
        assertEquals(Schema.Type.INT, z.getField("f1").schema().getType());
        assertEquals(Schema.Type.DOUBLE, z.getField("f2").schema().getType());
    }

    // test merging two maps
    @Test
    public void testMergeSchema5() throws IOException {
        Schema x, y, z;

        // element types are mergeable
        x = Schema.createArray(Schema.create(Schema.Type.INT));
        y = Schema.createArray(Schema.create(Schema.Type.DOUBLE));

        z = AvroStorageUtils.mergeSchema(x, y);
        assertEquals(Schema.Type.ARRAY, z.getType());
        assertEquals(Schema.Type.DOUBLE, z.getElementType().getType());

        // element types are not mergeable
        x = Schema.createArray(Schema.create(Schema.Type.INT));
        y = Schema.createArray(Schema.create(Schema.Type.BOOLEAN));

        try {
            z = AvroStorageUtils.mergeSchema(x, y);
            Assert.fail("exception is expected, but " + z.getType() + " is returned");
        } catch (IOException e) {
            assertEquals("Cannot merge "+ x.getElementType().getType() +
                         " with "+ y.getElementType().getType(), e.getMessage());
        }
    }

    // test merging two maps
    @Test
    public void testMergeSchema6() throws IOException {
        Schema x, y, z;

        // value types are mergeable
        x = Schema.createMap(Schema.create(Schema.Type.INT));
        y = Schema.createMap(Schema.create(Schema.Type.DOUBLE));

        z = AvroStorageUtils.mergeSchema(x, y);
        assertEquals(Schema.Type.MAP, z.getType());
        assertEquals(Schema.Type.DOUBLE, z.getValueType().getType());

        // value types are not mergeable
        x = Schema.createMap(Schema.create(Schema.Type.INT));
        y = Schema.createMap(Schema.create(Schema.Type.BOOLEAN));

        try {
            z = AvroStorageUtils.mergeSchema(x, y);
            Assert.fail("exception is expected, but " + z.getType() + " is returned");
        } catch (IOException e) {
            assertEquals("Cannot merge "+ x.getValueType().getType() +
                         " with "+ y.getValueType().getType(), e.getMessage());
        }
    }

    // test merging two unions
    @Test
    public void testMergeSchema7() throws IOException {
        Schema x, y, z;

        List<Schema> types1 = new ArrayList<Schema>();
        types1.add(Schema.create(Schema.Type.INT));
        List<Schema> types2 = new ArrayList<Schema>();
        types2.add(Schema.create(Schema.Type.DOUBLE));

        x = Schema.createUnion(types1);
        y = Schema.createUnion(types2);

        z = AvroStorageUtils.mergeSchema(x, y);
        assertEquals(Schema.Type.UNION, z.getType());
        assertTrue(z.getTypes().contains(types1.get(0)));
        assertTrue(z.getTypes().contains(types2.get(0)));
    }

    // test merging two fixeds
    @Test
    public void testMergeSchema8() throws IOException {
        Schema x, y, z;

        x = Schema.createFixed("fixed1", null, null, 1);
        y = Schema.createFixed("fixed2", null, null, 1);

        z = AvroStorageUtils.mergeSchema(x, y);
        assertEquals(Schema.Type.FIXED, z.getType());
        assertEquals(x.getFixedSize(), z.getFixedSize());

        x = Schema.createFixed("fixed1", null, null, 1);
        y = Schema.createFixed("fixed2", null, null, 2);

        try {
            z = AvroStorageUtils.mergeSchema(x, y);
            Assert.fail("exception is expected, but " + z.getType() + " is returned");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Cannot merge FIXED types with different sizes"));
        }
    }
}
