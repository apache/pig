/**
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
package org.apache.pig.backend.hadoop.accumulo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestAccumuloStorageConfiguration {

    protected Configuration original;
    protected AccumuloStorage storage;

    @Before
    public void setup() throws ParseException, IOException {
        storage = new AccumuloStorage();

        original = new Configuration();

        original.set("string1", "value1");
        original.set("string2", "value2");
        original.set("string3", "value3");
        original.setBoolean("boolean", true);
        original.setLong("long", 10);
        original.setInt("integer", 20);
    }

    protected Map<String, String> getContents(Configuration conf) {
        Map<String, String> contents = new HashMap<String, String>();
        Iterator<Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();

            contents.put(entry.getKey(), entry.getValue());
        }

        return contents;
    }

    @Test
    public void testClearEquivalenceStrings() {
        Configuration clearCopy = new Configuration(original);

        Assert.assertEquals(getContents(original), getContents(clearCopy));

        Map<String, String> entriesToUnset = new HashMap<String, String>();
        entriesToUnset.put("string1", "foo");
        entriesToUnset.put("string3", "bar");

        storage.clearUnset(clearCopy, entriesToUnset);

        Configuration originalCopy = new Configuration();
        for (Entry<String, String> entry : original) {
            if (!"string1".equals(entry.getKey())
                    && !"string3".equals(entry.getKey())) {
                originalCopy.set(entry.getKey(), entry.getValue());
            }
        }

        Assert.assertEquals(getContents(originalCopy), getContents(clearCopy));
    }

    @Test
    public void testClearEquivalenceOnTypes() {
        Configuration clearCopy = new Configuration(original);

        Assert.assertEquals(getContents(original), getContents(clearCopy));

        Map<String, String> entriesToUnset = new HashMap<String, String>();
        entriesToUnset.put("long", "foo");
        entriesToUnset.put("boolean", "bar");
        entriesToUnset.put("integer", "foobar");

        storage.clearUnset(clearCopy, entriesToUnset);

        Configuration originalCopy = new Configuration();
        for (Entry<String, String> entry : original) {
            if (!"long".equals(entry.getKey())
                    && !"boolean".equals(entry.getKey())
                    && !"integer".equals(entry.getKey())) {
                originalCopy.set(entry.getKey(), entry.getValue());
            }
        }

        Assert.assertEquals(getContents(originalCopy), getContents(clearCopy));
    }

    @Ignore
    // Ignored until dependency gets updated to Hadoop >=1.2.0 and we have
    // Configuration#unset
    @Test
    public void testUnsetEquivalenceStrings() {
        Configuration unsetCopy = new Configuration(original);

        Assert.assertEquals(getContents(original), getContents(unsetCopy));

        Map<String, String> entriesToUnset = new HashMap<String, String>();
        entriesToUnset.put("string1", "foo");
        entriesToUnset.put("string3", "bar");

        storage.simpleUnset(unsetCopy, entriesToUnset);

        Configuration originalCopy = new Configuration();
        for (Entry<String, String> entry : original) {
            if (!"string1".equals(entry.getKey())
                    && !"string2".equals(entry.getKey())) {
                originalCopy.set(entry.getKey(), entry.getValue());
            }
        }

        Assert.assertEquals(getContents(originalCopy), getContents(unsetCopy));
    }

    @Ignore
    // Ignored until dependency gets updated to Hadoop >=1.2.0 and we have
    // Configuration#unset
    @Test
    public void testEquivalenceStrings() {
        Configuration unsetCopy = new Configuration(original), clearCopy = new Configuration(
                original);

        Assert.assertEquals(getContents(unsetCopy), getContents(clearCopy));

        Map<String, String> entriesToUnset = new HashMap<String, String>();
        entriesToUnset.put("string1", "foo");
        entriesToUnset.put("string3", "bar");

        storage.simpleUnset(unsetCopy, entriesToUnset);
        storage.clearUnset(clearCopy, entriesToUnset);

        Assert.assertEquals(getContents(unsetCopy), getContents(clearCopy));

        Configuration originalCopy = new Configuration();
        for (Entry<String, String> entry : original) {
            if (!"string1".equals(entry.getKey())
                    && !"string2".equals(entry.getKey())) {
                originalCopy.set(entry.getKey(), entry.getValue());
            }
        }

        Assert.assertEquals(getContents(originalCopy), getContents(unsetCopy));
        Assert.assertEquals(getContents(originalCopy), getContents(clearCopy));
    }

    @Ignore
    // Ignored until dependency gets updated to Hadoop >=1.2.0 and we have
    // Configuration#unset
    @Test
    public void testEquivalenceOnTypes() {
        Configuration unsetCopy = new Configuration(original), clearCopy = new Configuration(
                original);

        Assert.assertEquals(getContents(original), getContents(unsetCopy));
        Assert.assertEquals(getContents(original), getContents(clearCopy));

        Map<String, String> entriesToUnset = new HashMap<String, String>();
        entriesToUnset.put("long", "foo");
        entriesToUnset.put("boolean", "bar");
        entriesToUnset.put("integer", "foobar");

        storage.simpleUnset(unsetCopy, entriesToUnset);
        storage.clearUnset(clearCopy, entriesToUnset);

        Configuration originalCopy = new Configuration();
        for (Entry<String, String> entry : original) {
            if (!"long".equals(entry.getKey())
                    && !"boolean".equals(entry.getKey())
                    && !"integer".equals(entry.getKey())) {
                originalCopy.set(entry.getKey(), entry.getValue());
            }
        }

        Assert.assertEquals(getContents(originalCopy), getContents(unsetCopy));
        Assert.assertEquals(getContents(originalCopy), getContents(clearCopy));
    }

    @Ignore
    // Ignored until dependency gets updated to Hadoop >=1.2.0 and we have
    // Configuration#unset
    @Test
    public void testUnsetEquivalenceOnTypes() {
        Configuration unsetCopy = new Configuration(original);

        Assert.assertEquals(getContents(original), getContents(unsetCopy));

        Map<String, String> entriesToUnset = new HashMap<String, String>();
        entriesToUnset.put("long", "foo");
        entriesToUnset.put("boolean", "bar");
        entriesToUnset.put("integer", "foobar");

        storage.simpleUnset(unsetCopy, entriesToUnset);

        Configuration originalCopy = new Configuration();
        for (Entry<String, String> entry : original) {
            if (!"long".equals(entry.getKey())
                    && !"boolean".equals(entry.getKey())
                    && !"integer".equals(entry.getKey())) {
                originalCopy.set(entry.getKey(), entry.getValue());
            }
        }

        Assert.assertEquals(getContents(originalCopy), getContents(unsetCopy));
    }
}
