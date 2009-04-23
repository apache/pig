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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

/** {@link org.apache.pig.backend.hadoop.hbase.HBaseStorage} Test Case **/
public class TestHBaseStorage extends TestCase {

    private static final Log LOG =
        LogFactory.getLog(TestHBaseStorage.class);
    
    private MiniCluster cluster = MiniCluster.buildCluster();
    private HBaseConfiguration conf;
    private MiniHBaseCluster hbaseCluster;
    
    private PigServer pig;
    
    final static int NUM_REGIONSERVERS = 1;
    
    // Test Table Inforamtions
    private static final String TESTTABLE = "pigtable";
    private static final String COLUMNFAMILY = "pig:";
    private static final String TESTCOLUMN_A = "pig:col_a";
    private static final String TESTCOLUMN_B = "pig:col_b";
    private static final String TESTCOLUMN_C = "pig:col_c";
    private static final HColumnDescriptor family =
        new HColumnDescriptor(COLUMNFAMILY);
    private static final int TEST_ROW_COUNT = 100;
    
    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        conf = new HBaseConfiguration(ConfigurationUtil.
             toConfiguration(cluster.getProperties()));
        try {
            hBaseClusterSetup();
        } catch (Exception e) {
            if(hbaseCluster != null) {
                hbaseCluster.shutdown();
            }
            throw e;
        }
        
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }
    
    /**
     * Actually start the MiniHBase instance.
     */
    protected void hBaseClusterSetup() throws Exception {
      // start the mini cluster
      hbaseCluster = new MiniHBaseCluster(conf, NUM_REGIONSERVERS);
      // opening the META table ensures that cluster is running
      new HTable(conf, HConstants.META_TABLE_NAME);
    }

    @Override
    protected void tearDown() throws Exception {
        // clear the table
        deleteTable();
        super.tearDown();
        try {
            HConnectionManager.deleteConnectionInfo(conf, true);
            if (hbaseCluster != null) {
                try {
                    hbaseCluster.shutdown();
                } catch (Exception e) {
                    LOG.warn("Closing mini hbase cluster", e);
                }
            }
        } catch (Exception e) {
            LOG.error(e);
        }
        pig.shutdown();
    }

    /**
     * load from hbase test
     * @throws IOException
     * @throws ExecException
     */
    @Test
    public void testLoadFromHBase() throws IOException, ExecException {
        prepareTable();
        pig.registerQuery("a = load 'hbase://" + TESTTABLE + "' using " +
            "org.apache.pig.backend.hadoop.hbase.HBaseStorage('" + TESTCOLUMN_A + 
            " " + TESTCOLUMN_B + " " + TESTCOLUMN_C + "') as (col_a, col_b:int, col_c);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBase Starting");
        while(it.hasNext()){
            Tuple t = it.next();
            LOG.info("LoadFromHBase "+ t);
            String col_a = ((DataByteArray)t.get(0)).toString();
            int col_b = (Integer)t.get(1);
            String col_c = ((DataByteArray)t.get(2)).toString();
            
            assertEquals(String.valueOf(count), col_a);
            assertEquals(count, col_b);
            assertEquals("TEXT" + count, col_c);
            
            count++;
        }
        assertEquals(TEST_ROW_COUNT, count);
        System.err.println("LoadFromHBase done");
    }

    /**
     * load from hbase test w/o hbase:// prefix
     * @throws IOException
     * @throws ExecException
     */
    @Test
    public void testBackwardsCompatibility() throws IOException, ExecException {
        prepareTable();
        pig.registerQuery("a = load '" + TESTTABLE + "' using " +
            "org.apache.pig.backend.hadoop.hbase.HBaseStorage('" + TESTCOLUMN_A + 
            " " + TESTCOLUMN_B + " " + TESTCOLUMN_C + "') as (col_a, col_b:int, col_c);");
        Iterator<Tuple> it = pig.openIterator("a");
        int count = 0;
        LOG.info("LoadFromHBase Starting");
        while(it.hasNext()){
            Tuple t = it.next();
            LOG.info("LoadFromHBase "+ t);
            String col_a = ((DataByteArray)t.get(0)).toString();
            int col_b = (Integer)t.get(1);
            String col_c = ((DataByteArray)t.get(2)).toString();
            
            assertEquals(String.valueOf(count), col_a);
            assertEquals(count, col_b);
            assertEquals("TEXT" + count, col_c);
            
            count++;
        }
        assertEquals(TEST_ROW_COUNT, count);
        System.err.println("LoadFromHBase done");
    }
    
    /**
     * Prepare a table in hbase for testing.
     * 
     * @throws IOException
     */
    private void prepareTable() throws IOException {
        // define the table schema
        HTableDescriptor tabledesc = new HTableDescriptor(TESTTABLE);
        tabledesc.addFamily(family);
        
        // create the table
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(TESTTABLE)) {
            deleteTable();
        }
        admin.createTable(tabledesc);
        
        // put some data into table
        HTable table = new HTable(conf, TESTTABLE);
        
        BatchUpdate batchUpdate;
        
        for(int i = 0 ; i < TEST_ROW_COUNT ; i++) {
            String v = Integer.toString(i);
            batchUpdate = new BatchUpdate(Bytes.toBytes(
                "00".substring(v.length()) + v));
            batchUpdate.put(TESTCOLUMN_A, Bytes.toBytes(v));
            batchUpdate.put(TESTCOLUMN_B, Bytes.toBytes(v));
            batchUpdate.put(TESTCOLUMN_C, Bytes.toBytes("TEXT" + i));
            table.commit(batchUpdate);
        }
    }
    
    private void deleteTable() throws IOException {
        // delete the table
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(TESTTABLE)) {
            admin.disableTable(TESTTABLE);
            while(admin.isTableEnabled(TESTTABLE)) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    // do nothing.
                }
            }
            admin.deleteTable(TESTTABLE);
        }
    }

}
