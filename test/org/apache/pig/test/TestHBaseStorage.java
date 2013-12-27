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
package org.apache.pig.test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestHBaseStorage {

    private static final Log LOG = LogFactory.getLog(TestHBaseStorage.class);
    private static HBaseTestingUtility util;
    private static Configuration conf;
    private static MiniCluster cluster;
    private static PigServer pig;

    enum DataFormat {
        HBaseBinary, UTF8PlainText,
    }

    // Test Table constants
    private static final String TESTTABLE_1 = "pigtable_1";
    private static final String TESTTABLE_2 = "pigtable_2";
    private static final byte[] COLUMNFAMILY = Bytes.toBytes("pig");
    private static final String TESTCOLUMN_A = "pig:col_a";
    private static final String TESTCOLUMN_B = "pig:col_b";
    private static final String TESTCOLUMN_C = "pig:col_c";

    private static final int TEST_ROW_COUNT = 100;

    @BeforeClass
    public static void setUp() throws Exception {
        // This is needed by Pig
        cluster = MiniCluster.buildCluster();
        conf = HBaseConfiguration.create(cluster.getConfiguration());

        util = new HBaseTestingUtility(conf);
        util.startMiniZKCluster();
        util.startMiniHBaseCluster(1, 1);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        // In HBase 0.90.1 and above we can use util.shutdownMiniHBaseCluster()
        // here instead.
        MiniHBaseCluster hbc = util.getHBaseCluster();
        if (hbc != null) {
            hbc.shutdown();
            hbc.join();
        }
        util.shutdownMiniZKCluster();
        cluster.shutDown();
    }


    @Before
    public void beforeTest() throws Exception {
        pig = new PigServer(ExecType.LOCAL, conf);
    }

    @After
    public void tearDown() throws Exception {
        try {
            deleteAllRows(TESTTABLE_1);
        } catch (IOException e) {}
        try {
            deleteAllRows(TESTTABLE_2);
        } catch (IOException e) {}
        pig.shutdown();
    }

    // DVR: I've found that it is faster to delete all rows in small tables
    // than to drop them.
    private void deleteAllRows(String tableName) throws Exception {
        HTable table = new HTable(conf, tableName);
        ResultScanner scanner = table.getScanner(new Scan());
        List<Delete> deletes = Lists.newArrayList();
        for (Result row : scanner) {
            deletes.add(new Delete(row.getRow()));
        }
        table.delete(deletes);
    }

    /**
     * Test Load from hbase with map parameters
     *
     */
    @Test
    public void testLoadWithMap_1() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A
                + " "
                + TESTCOLUMN_B
                + " "
                + TESTCOLUMN_C
                + " pig:"
                + "','-loadKey -cacheBlocks true') as (rowKey, col_a, col_b, col_c, pig_cf_map);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBase Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            LOG.info("LoadFromHBase " + t);
            String rowKey = ((DataByteArray) t.get(0)).toString();
            String col_a = ((DataByteArray) t.get(1)).toString();
            String col_b = ((DataByteArray) t.get(2)).toString();
            String col_c = ((DataByteArray) t.get(3)).toString();
            Map pig_cf_map = (Map) t.get(4);

            Assert.assertEquals("00".substring((count + "").length()) + count,
                    rowKey);
            Assert.assertEquals(count, Integer.parseInt(col_a));
            Assert.assertEquals(count + 0.0, Double.parseDouble(col_b), 1e-6);
            Assert.assertEquals("Text_" + count, col_c);

            Assert.assertEquals(5, pig_cf_map.size());
            Assert.assertEquals(count,
                    Integer.parseInt(pig_cf_map.get("col_a").toString()));
            Assert.assertEquals(count + 0.0,
                    Double.parseDouble(pig_cf_map.get("col_b").toString()), 1e-6);
            Assert.assertEquals("Text_" + count,
                    ((DataByteArray) pig_cf_map.get("col_c")).toString());
            Assert.assertEquals("PrefixedText_" + count,
                    ((DataByteArray) pig_cf_map.get("prefixed_col_d")).toString());

            count++;
        }
        Assert.assertEquals(TEST_ROW_COUNT, count);
        LOG.info("LoadFromHBase done");
    }

    /**
     * Test Load from hbase with maxTimestamp, minTimestamp, timestamp
     *
     */
    @Test
    public void testLoadWithSpecifiedTimestampAndRanges() throws IOException {
        long beforeTimeStamp = System.currentTimeMillis() - 10;

        HTable table = prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        long afterTimeStamp = System.currentTimeMillis() + 10;

        Assert.assertEquals("MaxTimestamp is set before rows added", 0, queryWithTimestamp(null , beforeTimeStamp, null));

        Assert.assertEquals("MaxTimestamp is set after rows added", TEST_ROW_COUNT, queryWithTimestamp(null, afterTimeStamp, null));

        Assert.assertEquals("MinTimestamp is set after rows added", 0, queryWithTimestamp(afterTimeStamp, null, null));

        Assert.assertEquals("MinTimestamp is set before rows added", TEST_ROW_COUNT, queryWithTimestamp(beforeTimeStamp, null, null));

        Assert.assertEquals("Timestamp range is set around rows added", TEST_ROW_COUNT, queryWithTimestamp(beforeTimeStamp, afterTimeStamp, null));

        Assert.assertEquals("Timestamp range is set after rows added", 0, queryWithTimestamp(afterTimeStamp, afterTimeStamp + 10, null));

        Assert.assertEquals("Timestamp range is set before rows added", 0, queryWithTimestamp(beforeTimeStamp - 10, beforeTimeStamp, null));

        Assert.assertEquals("Timestamp is set before rows added", 0, queryWithTimestamp(null, null, beforeTimeStamp));

        Assert.assertEquals("Timestamp is set after rows added", 0, queryWithTimestamp(null, null, afterTimeStamp));

        long specifiedTimestamp = table.get(new Get(Bytes.toBytes("00"))).getColumnLatest(COLUMNFAMILY, Bytes.toBytes("col_a")).getTimestamp();

        Assert.assertTrue("Timestamp is set equals to row 01", queryWithTimestamp(null, null, specifiedTimestamp) > 0);


        LOG.info("LoadFromHBase done");
    }

    private int queryWithTimestamp(Long minTimestamp, Long maxTimestamp, Long timestamp) throws IOException,
            ExecException {

        StringBuilder extraParams = new StringBuilder();

        if (minTimestamp != null){
            extraParams.append(" -minTimestamp " + minTimestamp + " ");
        }

        if (maxTimestamp != null){
            extraParams.append(" -maxTimestamp " + maxTimestamp + " ");
        }

        if (timestamp != null){
            extraParams.append(" -timestamp " + timestamp + " ");
        }


        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A
                + " "
                + TESTCOLUMN_B
                + " "
                + TESTCOLUMN_C
                + " pig:"
                + "','-loadKey " + extraParams.toString() + "') as (rowKey, col_a, col_b, col_c);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBase Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            LOG.info("LoadFromHBase " + t);
            String rowKey = ((DataByteArray) t.get(0)).toString();
            String col_a = ((DataByteArray) t.get(1)).toString();
            String col_b = ((DataByteArray) t.get(2)).toString();
            String col_c = ((DataByteArray) t.get(3)).toString();

            Assert.assertEquals("00".substring((count + "").length()) + count,
                    rowKey);
            Assert.assertEquals(count, Integer.parseInt(col_a));
            Assert.assertEquals(count + 0.0, Double.parseDouble(col_b), 1e-6);
            Assert.assertEquals("Text_" + count, col_c);

            count++;
        }
        return count;
    }

    /**
     *     * Test Load from hbase with map parameters and column prefix
     *
     */
    @Test
    public void testLoadWithMap_2_col_prefix() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + "pig:prefixed_col_*"
                + "','-loadKey') as (rowKey:chararray, pig_cf_map:map[]);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBase Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            LOG.info("LoadFromHBase " + t);
            String rowKey = t.get(0).toString();
            Map pig_cf_map = (Map) t.get(1);
            Assert.assertEquals(2, t.size());

            Assert.assertEquals("00".substring((count + "").length()) + count,
                    rowKey);
            Assert.assertEquals("PrefixedText_" + count,
                    ((DataByteArray) pig_cf_map.get("prefixed_col_d")).toString());
            Assert.assertEquals(1, pig_cf_map.size());

            count++;
        }
        Assert.assertEquals(TEST_ROW_COUNT, count);
        LOG.info("LoadFromHBase done");
    }

    /**
     * Test Load from hbase with map parameters and multiple column prefixs
     *
     */
    @Test
    public void testLoadWithMap_3_col_prefix() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + "pig:col_* pig:prefixed_col_*"
                + "','-loadKey') as (rowKey:chararray, pig_cf_map:map[], pig_prefix_cf_map:map[]);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBase Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            LOG.info("LoadFromHBase " + t);
            String rowKey = t.get(0).toString();
            Map pig_cf_map = (Map) t.get(1);
            Map pig_prefix_cf_map = (Map) t.get(2);
            Assert.assertEquals(3, t.size());

            Assert.assertEquals("00".substring((count + "").length()) + count,
                    rowKey);
            Assert.assertEquals("PrefixedText_" + count,
                    ((DataByteArray) pig_prefix_cf_map.get("prefixed_col_d")).toString());
            Assert.assertEquals(1, pig_prefix_cf_map.size());

            Assert.assertEquals(count,
                    Integer.parseInt(pig_cf_map.get("col_a").toString()));
            Assert.assertEquals(count + 0.0,
                    Double.parseDouble(pig_cf_map.get("col_b").toString()), 1e-6);
            Assert.assertEquals("Text_" + count,
                    ((DataByteArray) pig_cf_map.get("col_c")).toString());
            Assert.assertEquals(3, pig_cf_map.size());

            count++;
        }
        Assert.assertEquals(TEST_ROW_COUNT, count);
        LOG.info("LoadFromHBase done");
    }

    /**
     *     * Test Load from hbase with map parameters and column prefix with a
     *     static column
     *
     */
    @Test
    public void testLoadWithFixedAndPrefixedCols() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        //NOTE: I think there is some strangeness in how HBase handles column
        // filters. I was only able to reproduce a bug related to missing column
        // values in the response when I used 'sc' as a column name, instead of
        // 'col_a' as I use below.
        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + "pig:sc pig:prefixed_col_*"
                + "','-loadKey') as (rowKey:chararray, sc:chararray, pig_cf_map:map[]);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBase Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            LOG.info("LoadFromHBase " + t);
            String rowKey = (String) t.get(0);
            String col_a = t.get(1) != null ? t.get(1).toString() : null;
            Map pig_cf_map = (Map) t.get(2);
            Assert.assertEquals(3, t.size());

            Assert.assertEquals("00".substring((count + "").length()) + count,
                    rowKey);
            Assert.assertEquals("PrefixedText_" + count,
                    ((DataByteArray) pig_cf_map.get("prefixed_col_d")).toString());
            Assert.assertEquals(1, pig_cf_map.size());
            Assert.assertEquals(Integer.toString(count), col_a);

            count++;
        }
        Assert.assertEquals(TEST_ROW_COUNT, count);
        LOG.info("LoadFromHBase done");
    }

    /**
     *     * Test Load from hbase with map parameters and with a
     *     static column
     *
     */
    @Test
    public void testLoadWithFixedAndPrefixedCols2() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + "pig:col_a pig:prefixed_col_*"
                + "','-loadKey') as (rowKey:chararray, col_a:chararray, pig_cf_map:map[]);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBase Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            LOG.info("LoadFromHBase " + t);
            String rowKey = (String) t.get(0);
            String col_a = t.get(1) != null ? t.get(1).toString() : null;
            Map pig_cf_map = (Map) t.get(2);
            Assert.assertEquals(3, t.size());

            Assert.assertEquals("00".substring((count + "").length()) + count,
                    rowKey);
            Assert.assertEquals("PrefixedText_" + count,
                    ((DataByteArray) pig_cf_map.get("prefixed_col_d")).toString());
            Assert.assertEquals(1, pig_cf_map.size());
            Assert.assertEquals(Integer.toString(count), col_a);

            count++;
        }
        Assert.assertEquals(TEST_ROW_COUNT, count);
        LOG.info("LoadFromHBase done");
    }

    /**
     *     * Test Load from hbase with map parameters and with a
     *     static column in different order
     *
     */
    @Test
    public void testLoadOrderWithFixedAndPrefixedCols() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + "pig:col_* pig:prefixed_col_d"
                + "','-loadKey') as (rowKey:chararray, cols:map[], prefixed_col_d:chararray);");
        pig.registerQuery("b = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + "pig:prefixed_col_d pig:col_*"
                + "','-loadKey') as (rowKey:chararray, prefixed_col_d:chararray, cols:map[]);");
        Iterator<Tuple> it = pig.openIterator("a");
        Iterator<Tuple> it2 = pig.openIterator("b");
        int count = 0;
        LOG.info("LoadFromHBase Starting");
        while (it.hasNext() && it2.hasNext()) {
            Tuple t = it.next();
            Tuple t2 = it2.next();
            LOG.info("LoadFromHBase a:" + t);
            LOG.info("LoadFromHBase b:" + t2);
            String rowKey = (String) t.get(0);
            String rowKey2 = (String) t2.get(0);
            Assert.assertEquals(rowKey, rowKey2);
            Assert.assertEquals(t.size(), t2.size());
            @SuppressWarnings("rawtypes")
            Map cols_a = (Map) t.get(1);
            @SuppressWarnings("rawtypes")
            Map cols_b = (Map) t2.get(2);
            Assert.assertEquals(cols_a.size(), cols_b.size());
            count++;
        }
        Assert.assertEquals(TEST_ROW_COUNT, count);
        LOG.info("LoadFromHBase done");
    }

    /**
     * load from hbase test
     *
     * @throws IOException
     */
    @Test
    public void testLoadFromHBase() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);
        LOG.info("QUERY: " + "a = load 'hbase://" + TESTTABLE_1 + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A + " " + TESTCOLUMN_B + " " + TESTCOLUMN_C +" pig:col_d"
                + "') as (col_a, col_b, col_c, col_d);");
        pig.registerQuery("a = load 'hbase://" + TESTTABLE_1 + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A + " " + TESTCOLUMN_B + " " + TESTCOLUMN_C +" pig:col_d"
                + "') as (col_a, col_b, col_c, col_d);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBase Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            String col_a = ((DataByteArray) t.get(0)).toString();
            String col_b = ((DataByteArray) t.get(1)).toString();
            String col_c = ((DataByteArray) t.get(2)).toString();
            Object col_d = t.get(3);       // empty cell
            Assert.assertEquals(count, Integer.parseInt(col_a));
            Assert.assertEquals(count + 0.0, Double.parseDouble(col_b), 1e-6);
            Assert.assertEquals("Text_" + count, col_c);
            Assert.assertNull(col_d);
            count++;
        }
        Assert.assertEquals(TEST_ROW_COUNT, count);
        LOG.info("LoadFromHBase done");
    }

    /**
     * load from hbase test without hbase:// prefix
     *
     * @throws IOException
     */
    @Test
    public void testBackwardsCompatibility() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);
        pig.registerQuery("a = load '" + TESTTABLE_1 + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A + " " + TESTCOLUMN_B + " " + TESTCOLUMN_C
                + "') as (col_a, col_b, col_c);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("BackwardsCompatibility Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            String col_a = ((DataByteArray) t.get(0)).toString();
            String col_b = ((DataByteArray) t.get(1)).toString();
            String col_c = ((DataByteArray) t.get(2)).toString();

            Assert.assertEquals(count, Integer.parseInt(col_a));
            Assert.assertEquals(count + 0.0, Double.parseDouble(col_b), 1e-6);
            Assert.assertEquals("Text_" + count, col_c);
            count++;
        }
        Assert.assertEquals(TEST_ROW_COUNT, count);
        LOG.info("BackwardsCompatibility done");
    }

    /**
     * load from hbase test including the row key as the first column
     *
     * @throws IOException
     */
    @Test
    public void testLoadFromHBaseWithRowKey() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);
        pig.registerQuery("a = load 'hbase://" + TESTTABLE_1 + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A + " " + TESTCOLUMN_B + " " + TESTCOLUMN_C
                + "','-loadKey') as (rowKey,col_a, col_b, col_c);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBaseWithRowKey Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            String rowKey = ((DataByteArray) t.get(0)).toString();
            String col_a = ((DataByteArray) t.get(1)).toString();
            String col_b = ((DataByteArray) t.get(2)).toString();
            String col_c = ((DataByteArray) t.get(3)).toString();

            Assert.assertEquals("00".substring((count + "").length()) + count,
                    rowKey);
            Assert.assertEquals(count, Integer.parseInt(col_a));
            Assert.assertEquals(count + 0.0, Double.parseDouble(col_b), 1e-6);
            Assert.assertEquals("Text_" + count, col_c);

            count++;
        }
        Assert.assertEquals(TEST_ROW_COUNT, count);
        LOG.info("LoadFromHBaseWithRowKey done");
    }

    /**
     * Test Load from hbase with parameters lte and gte (01<=key<=98)
     *
     */
    @Test
    public void testLoadWithParameters_1() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A
                + " "
                + TESTCOLUMN_B
                + " "
                + TESTCOLUMN_C
                + "','-loadKey -gte 01 -lte 98') as (rowKey,col_a, col_b, col_c);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        int next = 1;
        LOG.info("LoadFromHBaseWithParameters_1 Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            LOG.info("LoadFromHBase " + t);
            String rowKey = ((DataByteArray) t.get(0)).toString();
            String col_a = ((DataByteArray) t.get(1)).toString();
            String col_b = ((DataByteArray) t.get(2)).toString();
            String col_c = ((DataByteArray) t.get(3)).toString();

            Assert.assertEquals("00".substring((next + "").length()) + next,
                    rowKey);
            Assert.assertEquals(next, Integer.parseInt(col_a));
            Assert.assertEquals(next + 0.0, Double.parseDouble(col_b), 1e-6);
            Assert.assertEquals("Text_" + next, col_c);

            count++;
            next++;
        }
        Assert.assertEquals(TEST_ROW_COUNT - 2, count);
        LOG.info("LoadFromHBaseWithParameters_1 done");
    }

    /**
     * Test Load from hbase with parameters lt and gt (00&lt;key&lt;99)
     */
    @Test
    public void testLoadWithParameters_2() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A
                + " "
                + TESTCOLUMN_B
                + " "
                + TESTCOLUMN_C
                + "','-loadKey -gt 00 -lt 99') as (rowKey,col_a, col_b, col_c);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        int next = 1;
        LOG.info("LoadFromHBaseWithParameters_2 Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            String rowKey = ((DataByteArray) t.get(0)).toString();
            String col_a = ((DataByteArray) t.get(1)).toString();
            String col_b = ((DataByteArray) t.get(2)).toString();
            String col_c = ((DataByteArray) t.get(3)).toString();

            Assert.assertEquals("00".substring((next + "").length()) + next,
                    rowKey);
            Assert.assertEquals(next, Integer.parseInt(col_a));
            Assert.assertEquals(next + 0.0, Double.parseDouble(col_b), 1e-6);
            Assert.assertEquals("Text_" + next, col_c);

            count++;
            next++;
        }
        Assert.assertEquals(TEST_ROW_COUNT - 2, count);
        LOG.info("LoadFromHBaseWithParameters_2 Starting");
    }

    /**
     * Test Load from hbase with parameters limit
     */
    @Test
    public void testLoadWithParameters_3() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);
        pig.registerQuery("a = load 'hbase://" + TESTTABLE_1 + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A + " " + TESTCOLUMN_B + " " + TESTCOLUMN_C
                + "','-loadKey -limit 10') as (rowKey,col_a, col_b, col_c);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBaseWithParameters_3 Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            String rowKey = ((DataByteArray) t.get(0)).toString();
            String col_a = ((DataByteArray) t.get(1)).toString();
            String col_b = ((DataByteArray) t.get(2)).toString();
            String col_c = ((DataByteArray) t.get(3)).toString();

            Assert.assertEquals("00".substring((count + "").length()) + count,
                    rowKey);
            Assert.assertEquals(count, Integer.parseInt(col_a));
            Assert.assertEquals(count + 0.0, Double.parseDouble(col_b), 1e-6);
            Assert.assertEquals("Text_" + count, col_c);

            count++;
        }
        // 'limit' apply for each region and here we have only one region
        Assert.assertEquals(10, count);
        LOG.info("LoadFromHBaseWithParameters_3 Starting");
    }

    /**
     * Test Load from hbase with parameters regex [2-3][4-5]
     *
     */
    @Test
    public void testLoadWithParameters_4() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A
                + " "
                + TESTCOLUMN_B
                + " "
                + TESTCOLUMN_C
                + "','-loadKey -regex [2-3][4-5]') as (rowKey,col_a, col_b, col_c);");
        Iterator<Tuple> it = pig.openIterator("a");

        int[] expectedValues = {24, 25, 34, 35};
        int count = 0;
        int countExpected = 4;
        LOG.info("LoadFromHBaseWithParameters_4 Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            LOG.info("LoadFromHBase " + t);
            String rowKey = ((DataByteArray) t.get(0)).toString();
            String col_a = ((DataByteArray) t.get(1)).toString();
            String col_b = ((DataByteArray) t.get(2)).toString();
            String col_c = ((DataByteArray) t.get(3)).toString();

            Assert.assertEquals(expectedValues[count] + "", rowKey);
            Assert.assertEquals(expectedValues[count], Integer.parseInt(col_a));
            Assert.assertEquals((double) expectedValues[count], Double.parseDouble(col_b), 1e-6);
            Assert.assertEquals("Text_" + expectedValues[count], col_c);

            count++;
        }
        Assert.assertEquals(countExpected, count);
        LOG.info("LoadFromHBaseWithParameters_4 done");
    }

    /**
     * Test Load from hbase with parameters lt and gt (10&lt;key&lt;30) and regex \\d[5]
     */
    @Test
    public void testLoadWithParameters_5() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);

        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A
                + " "
                + TESTCOLUMN_B
                + " "
                + TESTCOLUMN_C
                + "','-loadKey -gt 10 -lt 30 -regex \\\\d[5]') as (rowKey,col_a, col_b, col_c);");
        Iterator<Tuple> it = pig.openIterator("a");

        int[] expectedValues = {15, 25};
        int count = 0;
        int countExpected = 2;
        LOG.info("LoadFromHBaseWithParameters_5 Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            LOG.info("LoadFromHBase " + t);
            String rowKey = ((DataByteArray) t.get(0)).toString();
            String col_a = ((DataByteArray) t.get(1)).toString();
            String col_b = ((DataByteArray) t.get(2)).toString();
            String col_c = ((DataByteArray) t.get(3)).toString();

            Assert.assertEquals(expectedValues[count] + "", rowKey);
            Assert.assertEquals(expectedValues[count], Integer.parseInt(col_a));
            Assert.assertEquals((double) expectedValues[count], Double.parseDouble(col_b), 1e-6);
            Assert.assertEquals("Text_" + expectedValues[count], col_c);

            count++;
        }
        Assert.assertEquals(countExpected, count);
        LOG.info("LoadFromHBaseWithParameters_5 done");
    }

    /**
     * Test Load from hbase with projection.
     */
    @Test
    public void testLoadWithProjection_1() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.HBaseBinary);
        scanTable1(pig, DataFormat.HBaseBinary);
        pig.registerQuery("b = FOREACH a GENERATE col_a, col_c;");
        Iterator<Tuple> it = pig.openIterator("b");
        int index = 0;
        LOG.info("testLoadWithProjection_1 Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            int col_a = (Integer) t.get(0);
            String col_c = (String) t.get(1);
            Assert.assertEquals(index, col_a);
            Assert.assertEquals("Text_" + index, col_c);
            Assert.assertEquals(2, t.size());
            index++;
        }
        Assert.assertEquals(100, index);
        LOG.info("testLoadWithProjection_1 done");
    }

    /**
     * Test Load from hbase with projection and the default caster.
     */
    @Test
    public void testLoadWithProjection_2() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);
        scanTable1(pig, DataFormat.UTF8PlainText);
        pig.registerQuery("b = FOREACH a GENERATE col_a, col_c;");
        Iterator<Tuple> it = pig.openIterator("b");
        int index = 0;
        LOG.info("testLoadWithProjection_2 Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            int col_a = (Integer) t.get(0);
            String col_c = (String) t.get(1);
            Assert.assertEquals(index, col_a);
            Assert.assertEquals("Text_" + index, col_c);
            Assert.assertEquals(2, t.size());
            index++;
        }
        Assert.assertEquals(100, index);
        LOG.info("testLoadWithProjection_2 done");
    }

    /**
     * Test Load from hbase using HBaseBinaryConverter
     */
    @Test
    public void testHBaseBinaryConverter() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.HBaseBinary);

        scanTable1(pig, DataFormat.HBaseBinary);
        Iterator<Tuple> it = pig.openIterator("a");
        int index = 0;
        LOG.info("testHBaseBinaryConverter Starting");
        while (it.hasNext()) {
            Tuple t = it.next();
            String rowKey = (String) t.get(0);
            int col_a = (Integer) t.get(1);
            double col_b = (Double) t.get(2);
            String col_c = (String) t.get(3);

            Assert.assertEquals("00".substring((index + "").length()) + index,
                    rowKey);
            Assert.assertEquals(index, col_a);
            Assert.assertEquals(index + 0.0, col_b, 1e-6);
            Assert.assertEquals("Text_" + index, col_c);
            index++;
        }
        LOG.info("testHBaseBinaryConverter done");
    }

    /**
     * load from hbase 'TESTTABLE_1' using HBaseBinary format, and store it into
     * 'TESTTABLE_2' using HBaseBinaryFormat
     *
     * @throws IOException
     */
    @Test
    public void testStoreToHBase_1() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.HBaseBinary);
        prepareTable(TESTTABLE_2, false, DataFormat.HBaseBinary);

        pig.getPigContext().getProperties()
                .setProperty(MapReduceLauncher.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, "true");

        scanTable1(pig, DataFormat.HBaseBinary);
        pig.store("a", "hbase://" +  TESTTABLE_2,
                "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A + " " + TESTCOLUMN_B + " "
                + TESTCOLUMN_C + "','-caster HBaseBinaryConverter')");
        HTable table = new HTable(conf, TESTTABLE_2);
        ResultScanner scanner = table.getScanner(new Scan());
        Iterator<Result> iter = scanner.iterator();
        int i = 0;
        for (i = 0; iter.hasNext(); ++i) {
            Result result = iter.next();
            String v = i + "";
            String rowKey = Bytes.toString(result.getRow());
            int col_a = Bytes.toInt(getColValue(result, TESTCOLUMN_A));
            double col_b = Bytes.toDouble(getColValue(result, TESTCOLUMN_B));
            String col_c = Bytes.toString(getColValue(result, TESTCOLUMN_C));

            Assert.assertEquals("00".substring(v.length()) + v, rowKey);
            Assert.assertEquals(i, col_a);
            Assert.assertEquals(i + 0.0, col_b, 1e-6);
            Assert.assertEquals("Text_" + i, col_c);
        }
        Assert.assertEquals(100, i);

        pig.getPigContext().getProperties()
                .setProperty(MapReduceLauncher.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, "false");
    }

    /**
     * load from hbase 'TESTTABLE_1' using HBaseBinary format, and store it into
     * 'TESTTABLE_2' using HBaseBinaryFormat projecting out column c
     *
     * @throws IOException
     */
    @Test
    public void testStoreToHBase_1_with_projection() throws IOException {
        System.getProperties().setProperty("pig.usenewlogicalplan", "false");
        prepareTable(TESTTABLE_1, true, DataFormat.HBaseBinary);
        prepareTable(TESTTABLE_2, false, DataFormat.HBaseBinary);
        scanTable1(pig, DataFormat.HBaseBinary);
        pig.registerQuery("b = FOREACH a GENERATE rowKey, col_a, col_b;");
        pig.store("b",  TESTTABLE_2,
                "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A + " " + TESTCOLUMN_B +
                "','-caster HBaseBinaryConverter')");

        HTable table = new HTable(conf, TESTTABLE_2);
        ResultScanner scanner = table.getScanner(new Scan());
        Iterator<Result> iter = scanner.iterator();
        int i = 0;
        for (i = 0; iter.hasNext(); ++i) {
            Result result = iter.next();
            String v = String.valueOf(i);
            String rowKey = Bytes.toString(result.getRow());
            int col_a = Bytes.toInt(getColValue(result, TESTCOLUMN_A));
            double col_b = Bytes.toDouble(getColValue(result, TESTCOLUMN_B));

            Assert.assertEquals("00".substring(v.length()) + v, rowKey);
            Assert.assertEquals(i, col_a);
            Assert.assertEquals(i + 0.0, col_b, 1e-6);
        }
        Assert.assertEquals(100, i);
    }

    /**
     * load from hbase 'TESTTABLE_1' using HBaseBinary format, and store it into
     * 'TESTTABLE_2' using UTF-8 Plain Text format
     *
     * @throws IOException
     */
    @Test
    public void testStoreToHBase_2() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.HBaseBinary);
        prepareTable(TESTTABLE_2, false, DataFormat.HBaseBinary);
        scanTable1(pig, DataFormat.HBaseBinary);
        pig.store("a", TESTTABLE_2,
                "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A + " " + TESTCOLUMN_B + " "
                + TESTCOLUMN_C + "')");

        HTable table = new HTable(conf, TESTTABLE_2);
        ResultScanner scanner = table.getScanner(new Scan());
        Iterator<Result> iter = scanner.iterator();
        int i = 0;
        for (i = 0; iter.hasNext(); ++i) {
            Result result = iter.next();
            String v = i + "";
            String rowKey = new String(result.getRow());
            int col_a = Integer.parseInt(new String(getColValue(result, TESTCOLUMN_A)));
            double col_b = Double.parseDouble(new String(getColValue(result, TESTCOLUMN_B)));
            String col_c = new String(getColValue(result, TESTCOLUMN_C));

            Assert.assertEquals("00".substring(v.length()) + v, rowKey);
            Assert.assertEquals(i, col_a);
            Assert.assertEquals(i + 0.0, col_b, 1e-6);
            Assert.assertEquals("Text_" + i, col_c);
        }
        Assert.assertEquals(100, i);
    }

    /**
     * Assert that -noWAL actually disables the WAL
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testNoWAL() throws IOException, ParseException {
        HBaseStorage hbaseStorage = new HBaseStorage(TESTCOLUMN_A, "-noWAL");

        Object key = "somekey";
        byte type = DataType.CHARARRAY;
        Assert.assertFalse(hbaseStorage.createPut(key, type).getWriteToWAL());
    }

    /**
     * Assert that without -noWAL, the WAL is enabled the WAL
     * @throws IOException
     * @throws ParseException
     */
    @Test
    public void testWIthWAL() throws IOException, ParseException {
        HBaseStorage hbaseStorage = new HBaseStorage(TESTCOLUMN_A);

        Object key = "somekey";
        byte type = DataType.CHARARRAY;
        Assert.assertTrue(hbaseStorage.createPut(key, type).getWriteToWAL());
    }

    /**
     * load from hbase 'TESTTABLE_1' using HBaseBinary format, and store it into
     * 'TESTTABLE_2' using UTF-8 Plain Text format projecting column c
     *
     * @throws IOException
     */
    @Test
    public void testStoreToHBase_2_with_projection() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.HBaseBinary);
        prepareTable(TESTTABLE_2, false, DataFormat.UTF8PlainText);
        scanTable1(pig, DataFormat.HBaseBinary);
        pig.registerQuery("b = FOREACH a GENERATE rowKey, col_a, col_b;");
        pig.store("b", TESTTABLE_2,
                "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A + " " + TESTCOLUMN_B + "')");

        HTable table = new HTable(conf, TESTTABLE_2);
        ResultScanner scanner = table.getScanner(new Scan());
        Iterator<Result> iter = scanner.iterator();
        int i = 0;
        for (i = 0; iter.hasNext(); ++i) {
            Result result = iter.next();
            String v = i + "";
            String rowKey = new String(result.getRow());
            int col_a = Integer.parseInt(new String(getColValue(result, TESTCOLUMN_A)));
            double col_b = Double.parseDouble(new String(getColValue(result, TESTCOLUMN_B)));

            Assert.assertEquals("00".substring(v.length()) + v, rowKey);
            Assert.assertEquals(i, col_a);
            Assert.assertEquals(i + 0.0, col_b, 1e-6);
        }
        Assert.assertEquals(100, i);
    }

    /**
     * load from hbase 'TESTTABLE_1' using UTF-8 Plain Text format, and store it
     * into 'TESTTABLE_2' using UTF-8 Plain Text format projecting column c
     *
     * @throws IOException
     */
    @Test
    public void testStoreToHBase_3_with_projection_no_caster() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.UTF8PlainText);
        prepareTable(TESTTABLE_2, false, DataFormat.UTF8PlainText);
        scanTable1(pig, DataFormat.UTF8PlainText);
        pig.registerQuery("b = FOREACH a GENERATE rowKey, col_a, col_b;");
        pig.store("b", TESTTABLE_2,
                "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A + " " + TESTCOLUMN_B + "')");

        HTable table = new HTable(conf, TESTTABLE_2);
        ResultScanner scanner = table.getScanner(new Scan());
        Iterator<Result> iter = scanner.iterator();
        int i = 0;
        for (i = 0; iter.hasNext(); ++i) {
            Result result = iter.next();
            String v = i + "";
            String rowKey = new String(result.getRow());

            String col_a = new String(getColValue(result, TESTCOLUMN_A));
            String col_b = new String(getColValue(result, TESTCOLUMN_B));

            Assert.assertEquals("00".substring(v.length()) + v, rowKey);
            Assert.assertEquals(i + "", col_a);
            Assert.assertEquals(i + 0.0 + "", col_b);
        }
        Assert.assertEquals(100, i);
    }

    /**
     * Test to if HBaseStorage handles different scans in a single MR job.
     * This can happen PIG loads two different aliases (as in a join or
     * union).
     */
    @Test
    public void testHeterogeneousScans() throws IOException {
        prepareTable(TESTTABLE_1, true, DataFormat.HBaseBinary);
        prepareTable(TESTTABLE_2, true, DataFormat.UTF8PlainText);
        scanTable1(pig, DataFormat.HBaseBinary);
        pig.registerQuery(String.format(
              " b = load 'hbase://%s' using %s('%s %s') as (col_a:int, col_c);",
              TESTTABLE_2, "org.apache.pig.backend.hadoop.hbase.HBaseStorage",
              TESTCOLUMN_A, TESTCOLUMN_C));
        pig.registerQuery(" c = join a by col_a, b by col_a; ");
        // this results in a single job with mappers loading
        // different HBaseStorage specs.

        Iterator<Tuple> it = pig.openIterator("c");
        int index = 0;
        while (it.hasNext()) {
            Tuple t = it.next();
            String rowKey = (String) t.get(0);
            int col_a = (Integer) t.get(1);
            Assert.assertNotNull(t.get(2));
            double col_b = (Double) t.get(2);
            String col_c = (String) t.get(3);

            Assert.assertEquals("00".substring((index + "").length()) + index,
                    rowKey);
            Assert.assertEquals(index, col_a);
            Assert.assertEquals(index + 0.0, col_b, 1e-6);
            Assert.assertEquals("Text_" + index, col_c);
            index++;
        }
        Assert.assertEquals(index, TEST_ROW_COUNT);
    }

    private void scanTable1(PigServer pig, DataFormat dataFormat) throws IOException {
        scanTable1(pig, dataFormat, "");
    }

    private void scanTable1(PigServer pig, DataFormat dataFormat, String extraParams) throws IOException {
        pig.registerQuery("a = load 'hbase://"
                + TESTTABLE_1
                + "' using "
                + "org.apache.pig.backend.hadoop.hbase.HBaseStorage('"
                + TESTCOLUMN_A
                + " "
                + TESTCOLUMN_B
                + " "
                + TESTCOLUMN_C
                + "','-loadKey "
                + (dataFormat == DataFormat.HBaseBinary ? " -caster HBaseBinaryConverter" : "")
                + " " + extraParams + " "
                + "') as (rowKey:chararray,col_a:int, col_b:double, col_c:chararray);");
    }

    /**
     * Prepare a table in hbase for testing.
     *
     */
    private HTable prepareTable(String tableName, boolean initData,
            DataFormat format) throws IOException {
        // define the table schema
        HTable table = null;
        try {
            deleteAllRows(tableName);
        } catch (Exception e) {
            // It's ok, table might not exist.
        }
        try {
        table = util.createTable(Bytes.toBytesBinary(tableName),
                COLUMNFAMILY);
        } catch (Exception e) {
            table = new HTable(conf, Bytes.toBytesBinary(tableName));
        }

        if (initData) {
            for (int i = 0; i < TEST_ROW_COUNT; i++) {
                String v = i + "";
                if (format == DataFormat.HBaseBinary) {
                    // row key: string type
                    Put put = new Put(Bytes.toBytes("00".substring(v.length())
                            + v));
                    // sc: int type
                    put.add(COLUMNFAMILY, Bytes.toBytes("sc"),
                            Bytes.toBytes(i));
                    // col_a: int type
                    put.add(COLUMNFAMILY, Bytes.toBytes("col_a"),
                            Bytes.toBytes(i));
                    // col_b: double type
                    put.add(COLUMNFAMILY, Bytes.toBytes("col_b"),
                            Bytes.toBytes(i + 0.0));
                    // col_c: string type
                    put.add(COLUMNFAMILY, Bytes.toBytes("col_c"),
                            Bytes.toBytes("Text_" + i));
                    // prefixed_col_d: string type
                    put.add(COLUMNFAMILY, Bytes.toBytes("prefixed_col_d"),
                            Bytes.toBytes("PrefixedText_" + i));
                    table.put(put);
                } else {
                    // row key: string type
                    Put put = new Put(
                            ("00".substring(v.length()) + v).getBytes());
                    // sc: int type
                    put.add(COLUMNFAMILY, Bytes.toBytes("sc"),
                            (i + "").getBytes()); // int
                    // col_a: int type
                    put.add(COLUMNFAMILY, Bytes.toBytes("col_a"),
                            (i + "").getBytes()); // int
                    // col_b: double type
                    put.add(COLUMNFAMILY, Bytes.toBytes("col_b"),
                            ((i + 0.0) + "").getBytes());
                    // col_c: string type
                    put.add(COLUMNFAMILY, Bytes.toBytes("col_c"),
                            ("Text_" + i).getBytes());
                    // prefixed_col_d: string type
                    put.add(COLUMNFAMILY, Bytes.toBytes("prefixed_col_d"),
                            ("PrefixedText_" + i).getBytes());
                    table.put(put);
                }
            }
            table.flushCommits();
        }
        return table;
    }

    /**
     * Helper to deal with fetching a result based on a cf:colname string spec
     * @param result
     * @param colName
     * @return
     */
    private static byte[] getColValue(Result result, String colName) {
        byte[][] colArray = Bytes.toByteArrays(colName.split(":"));
        return result.getValue(colArray[0], colArray[1]);

    }
}
