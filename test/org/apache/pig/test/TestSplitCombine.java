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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Before;
import org.junit.Test;

import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.metadata.BlockMetaData;

public class TestSplitCombine {
    private Configuration conf;
    private TestPigInputFormat pigInputFormat;
    private ArrayList<OperatorKey> ok;

    class TestPigInputFormat extends PigInputFormat {
        public List<InputSplit> getPigSplits(List<InputSplit> oneInputSplits,
                        int inputIndex, ArrayList<OperatorKey> targetOps,
                        Path path, boolean combinable, Configuration conf)
                        throws IOException, InterruptedException {
            return super.getPigSplits(oneInputSplits, inputIndex, targetOps,
                            1000, combinable, conf);
        }
    }

    class DummyInputSplit extends InputSplit {
        private final long length;
        private final String[] locations;

        public DummyInputSplit(long len, String[] locs) {
            length = len;
            locations = locs;
        }

        @Override
        public long getLength() {
            return length;
        }

        @Override
        public String[] getLocations() {
            return locations;
        }
    }

    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.setLong("pig.maxCombinedSplitSize", 1000);
      conf.setStrings("io.serializations",
            "org.apache.hadoop.io.serializer.WritableSerialization");
        pigInputFormat = new TestPigInputFormat();
        ok = new ArrayList<OperatorKey>();
        ok.add(new OperatorKey());
    }

    @Test
    public void test1() throws IOException, InterruptedException {
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
        rawSplits.add(new DummyInputSplit(500, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(400, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(400, new String[] {
                        "l1", "l4", "l5"
        }));
        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                        null, true, conf);
        Assert.assertEquals(result.size(), 2);
        int index = 0;
        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;
            int len = pigSplit.getNumPaths();
            if (index == 0) {
                Assert.assertEquals(2, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(500, pigSplit.getLength(0));
                Assert.assertEquals(400, pigSplit.getLength(1));
            }
            else {
                Assert.assertEquals(1, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l4", "l5"
                });
                Assert.assertEquals(400, pigSplit.getLength(0));
            }
            index++;
        }
    }

    @Test
    public void test2() throws IOException, InterruptedException {
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
        rawSplits.add(new DummyInputSplit(600, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(700, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(800, new String[] {
                        "l1", "l4", "l5"
        }));
        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                        null, true, conf);
        Assert.assertEquals(result.size(), 3);
        int index = 0;
        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;
            int len = pigSplit.getNumPaths();
            if (index == 0) {
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l4", "l5"
                });
                Assert.assertEquals(1, len);
                Assert.assertEquals(800, pigSplit.getLength(0));
            }
            else if (index == 1) {
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(1, len);
                Assert.assertEquals(700, pigSplit.getLength(0));
            }
            else {
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(1, len);
                Assert.assertEquals(600, pigSplit.getLength(0));
            }
            index++;
        }
    }

    @Test
    public void test3() throws IOException, InterruptedException {
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
        rawSplits.add(new DummyInputSplit(500, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(200, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(100, new String[] {
                        "l1", "l4", "l5"
        }));
        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                        null, true, conf);
        Assert.assertEquals(1, result.size());
        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;
            int len = pigSplit.getNumPaths();
            Assert.assertEquals(3, len);
            checkLocations(pigSplit.getLocations(), new String[] {
                            "l1", "l2", "l3", "l4", "l5"
            });
            Assert.assertEquals(500, pigSplit.getLength(0));
            Assert.assertEquals(200, pigSplit.getLength(1));
            Assert.assertEquals(100, pigSplit.getLength(2));
        }
    }

    @Test
    public void test4() throws IOException, InterruptedException {
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
        rawSplits.add(new DummyInputSplit(500, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(200, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(100, new String[] {
                        "l1", "l4", "l5"
        }));
        rawSplits.add(new DummyInputSplit(100, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(200, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(500, new String[] {
                        "l1", "l4", "l5"
        }));
        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                        null, true, conf);
        Assert.assertEquals(2, result.size());
        int idx = 0;
        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;
            int len = pigSplit.getNumPaths();
            if (idx == 0) {
                Assert.assertEquals(2, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l4", "l5"
                });
                Assert.assertEquals(500, pigSplit.getLength(0));
                Assert.assertEquals(100, pigSplit.getLength(1));
            }
            else {
                Assert.assertEquals(4, len);
                Assert.assertEquals(500, pigSplit.getLength(0));
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(200, pigSplit.getLength(1));
                Assert.assertEquals(200, pigSplit.getLength(2));
                Assert.assertEquals(100, pigSplit.getLength(3));
            }
            idx++;
        }
    }

    @Test
    public void test5() throws IOException, InterruptedException {
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
        rawSplits.add(new DummyInputSplit(500, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(400, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(400, new String[] {
                        "l1", "l4", "l5"
        }));
        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                        null, false, conf);
        Assert.assertEquals(3, result.size());
        int index = 0;
        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;
            int len = pigSplit.getNumPaths();
            if (index == 0) {
                Assert.assertEquals(1, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(500, pigSplit.getLength(0));
            }
            else if (index == 1) {
                Assert.assertEquals(1, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(400, pigSplit.getLength(0));
            }
            else {
                Assert.assertEquals(1, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l4", "l5"
                });
                Assert.assertEquals(400, pigSplit.getLength(0));
            }
            index++;
        }
    }

    @Test
    public void test6() throws IOException, InterruptedException {
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
        rawSplits.add(new DummyInputSplit(600, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(500, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(400, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(300, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(200, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(100, new String[] {
                        "l1", "l2", "l3"
        }));
        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                        null, true, conf);
        Assert.assertEquals(3, result.size());
        int idx = 0;
        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;
            int len = pigSplit.getNumPaths();
            if (idx == 0) {
                Assert.assertEquals(2, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(600, pigSplit.getLength(0));
                Assert.assertEquals(400, pigSplit.getLength(1));
            }
            else if (idx == 1) {
                Assert.assertEquals(3, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(500, pigSplit.getLength(0));
                Assert.assertEquals(300, pigSplit.getLength(1));
                Assert.assertEquals(200, pigSplit.getLength(2));
            }
            else {
                Assert.assertEquals(1, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(100, pigSplit.getLength(0));
            }
            idx++;
        }
    }

    @Test
    public void test7() throws IOException, InterruptedException {
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
        rawSplits.add(new DummyInputSplit(100, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(200, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(300, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(400, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(500, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(600, new String[] {
                        "l1", "l2", "l3"
        }));
        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                        null, true, conf);
        Assert.assertEquals(3, result.size());
        int idx = 0;
        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;
            int len = pigSplit.getNumPaths();
            if (idx == 0) {
                Assert.assertEquals(2, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(600, pigSplit.getLength(0));
                Assert.assertEquals(400, pigSplit.getLength(1));
            }
            else if (idx == 1) {
                Assert.assertEquals(3, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(500, pigSplit.getLength(0));
                Assert.assertEquals(300, pigSplit.getLength(1));
                Assert.assertEquals(200, pigSplit.getLength(2));
            }
            else {
                Assert.assertEquals(1, len);
                checkLocations(pigSplit.getLocations(), new String[] {
                                "l1", "l2", "l3"
                });
                Assert.assertEquals(100, pigSplit.getLength(0));
            }
            idx++;
        }
    }

    @Test
    public void test8() throws IOException, InterruptedException {
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
        rawSplits.add(new DummyInputSplit(100, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(100, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(200, new String[] {
                        "l1", "l4", "l5"
        }));
        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                        null, true, conf);
        Assert.assertEquals(result.size(), 1);
        int index = 0;
        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;
            int len = pigSplit.getNumPaths();
            Assert.assertEquals(3, len);
            checkLocations(pigSplit.getLocations(), new String[] {
                            "l1", "l2", "l3", "l4", "l5"
            });
            Assert.assertEquals(200, pigSplit.getLength(0));
            Assert.assertEquals(100, pigSplit.getLength(1));
            Assert.assertEquals(100, pigSplit.getLength(2));
            index++;
        }
    }

    @Test
    public void test9() throws IOException, InterruptedException {
        // verify locations in order
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();
        rawSplits.add(new DummyInputSplit(100, new String[] {
                        "l1", "l2", "l3"
        }));
        rawSplits.add(new DummyInputSplit(200, new String[] {
                        "l3", "l4", "l5"
        }));
        rawSplits.add(new DummyInputSplit(400, new String[] {
                        "l5", "l6", "l1"
        }));
        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                        null, true, conf);
        Assert.assertEquals(result.size(), 1);
        int index = 0;
        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;
            int len = pigSplit.getNumPaths();
            Assert.assertEquals(3, len);
            // only 5 locations are in list: refer to PIG-1648 for more details
            checkLocationOrdering(pigSplit.getLocations(), new String[] {
                            "l5", "l1", "l6", "l3", "l4"
            });
            Assert.assertEquals(400, pigSplit.getLength(0));
            Assert.assertEquals(200, pigSplit.getLength(1));
            Assert.assertEquals(100, pigSplit.getLength(2));
            index++;
        }
    }
    
    @Test
    public void test10() throws IOException, InterruptedException {
        // verify locations in order
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();

        rawSplits.add(new FileSplit(new Path("path1"), 0, 100, new String[] {
                "l1", "l2", "l3" }));
        rawSplits.add(new FileSplit(new Path("path2"), 0, 200, new String[] {
                "l3", "l4", "l5" }));
        rawSplits.add(new FileSplit(new Path("path3"), 0, 400, new String[] {
                "l5", "l6", "l1" }));
        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                null, true, conf);

        Assert.assertEquals(result.size(), 1);

        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;
            // write to a byte array output stream
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            DataOutput out = new DataOutputStream(outputStream);
            pigSplit.write(out);
            // restore the pig split from the byte array
            ByteArrayInputStream inputStream = new ByteArrayInputStream(
                    outputStream.toByteArray());

            DataInput in = new DataInputStream(inputStream);
            PigSplit anotherSplit = new PigSplit();
            anotherSplit.setConf(conf);

            anotherSplit.readFields(in);

            Assert.assertEquals(700, anotherSplit.getLength());
            checkLocationOrdering(pigSplit.getLocations(), new String[] { "l5",
                    "l1", "l6", "l3", "l4" });

            Assert.assertEquals(3, anotherSplit.getNumPaths());

            Assert.assertEquals(
                    "org.apache.hadoop.mapreduce.lib.input.FileSplit",
                    (anotherSplit.getWrappedSplit(0).getClass().getName()));
            Assert.assertEquals(
                    "org.apache.hadoop.mapreduce.lib.input.FileSplit",
                    (anotherSplit.getWrappedSplit(1).getClass().getName()));
            Assert.assertEquals(
                    "org.apache.hadoop.mapreduce.lib.input.FileSplit",
                    (anotherSplit.getWrappedSplit(2).getClass().getName()));
        }
    }

    @Test
    public void test11() throws IOException, InterruptedException {
        // verify locations in order
        ArrayList<InputSplit> rawSplits = new ArrayList<InputSplit>();

        // first split is parquetinputsplit
        rawSplits.add(new ParquetInputSplit(new Path("path1"), 0, 100,
                new String[] { "l1", "l2", "l3" },
                new ArrayList<BlockMetaData>(), "", "",
                new HashMap<String, String>(), new HashMap<String, String>()));
        // second split is file split
        rawSplits.add(new FileSplit(new Path("path2"), 0, 400, new String[] {
                "l5", "l6", "l1" }));

        List<InputSplit> result = pigInputFormat.getPigSplits(rawSplits, 0, ok,
                null, true, conf);

        // pig combines two into one pigsplit
        Assert.assertEquals(result.size(), 1);

        for (InputSplit split : result) {
            PigSplit pigSplit = (PigSplit) split;

            // write to a byte array output stream
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            DataOutput out = new DataOutputStream(outputStream);
            pigSplit.write(out);
            // restore the pig split from the byte array
            ByteArrayInputStream inputStream = new ByteArrayInputStream(
                    outputStream.toByteArray());

            DataInput in = new DataInputStream(inputStream);
            PigSplit anotherSplit = new PigSplit();
            anotherSplit.setConf(conf);
            anotherSplit.readFields(in);

            Assert.assertEquals(500, anotherSplit.getLength());

            Assert.assertEquals(2, anotherSplit.getNumPaths());
            Assert.assertEquals("parquet.hadoop.ParquetInputSplit",
                    (anotherSplit.getWrappedSplit(0).getClass().getName()));
            Assert.assertEquals(
                    "org.apache.hadoop.mapreduce.lib.input.FileSplit",
                    (anotherSplit.getWrappedSplit(1).getClass().getName()));
        }
    }
    
    private void checkLocations(String[] actual, String[] expected) {
        HashSet<String> expectedSet = new HashSet<String>();
        for (String str : expected)
            expectedSet.add(str);
        int count = 0;
        for (String str : actual) {
            if (expectedSet.contains(str)) count++;
        }
        Assert.assertEquals(count, expected.length);
    }

    private void checkLocationOrdering(String[] actual, String[] expected) {
      Assert.assertEquals(expected.length, actual.length);
      for (int i = 0; i < actual.length; i++)
        Assert.assertEquals(expected[i], actual[i]);
    }
}
