/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.accumulo;

import org.apache.pig.backend.hadoop.accumulo.Column.Type;
import org.junit.Assert;
import org.junit.Test;

public class TestAccumuloColumns {

    @Test(expected = NullPointerException.class)
    public void testNull() {
        new Column(null);
    }

    @Test
    public void testEmptyColumn() {
        Column c = new Column("");

        Assert.assertEquals(Type.LITERAL, c.getType());
        Assert.assertEquals("", c.getColumnFamily());
        Assert.assertEquals(null, c.getColumnQualifier());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBlankColfamQual() {
        new Column(":");
    }

    @Test
    public void testColfamWithColqualRegex() {
        Column c = new Column("cf:*");

        Assert.assertEquals(Type.COLQUAL_PREFIX, c.getType());
        Assert.assertEquals("cf", c.getColumnFamily());
        Assert.assertEquals("", c.getColumnQualifier());
    }

    @Test
    public void testColfamRegexEmptyColqual() {
        Column c = new Column("cf:");

        Assert.assertEquals(Type.COLQUAL_PREFIX, c.getType());
        Assert.assertEquals("cf", c.getColumnFamily());
        Assert.assertEquals("", c.getColumnQualifier());
    }

    @Test
    public void testColfamRegex() {
        Column c = new Column("cf*:");

        Assert.assertEquals(Type.COLFAM_PREFIX, c.getType());
        Assert.assertEquals("cf", c.getColumnFamily());
        Assert.assertEquals("", c.getColumnQualifier());
    }

    @Test
    public void testColfamRegexColqualRegex() {
        // TODO Change test when cf*:cq* is supported
        Column c = new Column("cf*:cq*");

        Assert.assertEquals(Type.COLFAM_PREFIX, c.getType());
        Assert.assertEquals("cf", c.getColumnFamily());
        Assert.assertEquals("cq*", c.getColumnQualifier());
    }
}
