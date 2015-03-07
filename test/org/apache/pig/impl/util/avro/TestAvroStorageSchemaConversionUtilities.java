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
package org.apache.pig.impl.util.avro;

import org.apache.avro.Schema;
import org.apache.pig.ResourceSchema;
import org.junit.Assert;
import org.junit.Test;


import java.io.File;
import java.io.IOException;

public class TestAvroStorageSchemaConversionUtilities {
    final private static String BASE_DIR = "test/org/apache/pig/builtin/avro/schema/";

    @Test
    public void testNullableRecordInMap() throws IOException {
        Assert.assertEquals(
                "key:chararray,value:int,parameters:[nullable_record:(id:chararray)]",
                parse(BASE_DIR + "nullableRecordInMap.avsc", true));
    }

    @Test
    public void testNullableArrayInMap() throws IOException {
        Assert.assertEquals(
                "key:chararray,value:int,parameters:[array:{(chararray)}]",
                parse(BASE_DIR + "nullableArrayInMap.avsc", true));
    }

    private static String parse(String schema, boolean recursive) throws IOException {
        final Schema s = new Schema.Parser().parse(new File(schema));
        final ResourceSchema resourceSchema = AvroStorageSchemaConversionUtilities.avroSchemaToResourceSchema(s, recursive);
        return resourceSchema.toString();
    }
}
