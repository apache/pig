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

package org.apache.hadoop.zebra.pig;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TestBasicTable;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMapSideCoGroup extends BaseTestCase {
    private static Path table1, table2;
    private static Configuration conf;

    @BeforeClass
    public static void setUp() throws Exception {
        init();
        TestBasicTable.setUpOnce();
        conf = TestBasicTable.conf;
        table1 = getTableFullPath( "TestMapSideCoGroup1" );
        removeDir( table1 );
        table2 = getTableFullPath( "TestMapSideCoGroup2" );
        removeDir( table2 );
    }

    @AfterClass
    public static void tearDown() throws Exception {
        pigServer.shutdown();
    }

    @Test
    public void test() throws IOException {
        int table1RowCount = 100000;
        int table2RowCount = 200000;
        int table1DupFactor = 15;
        int table2DupFactor = 125;
        createTable( table1RowCount, table1DupFactor, "a:int, b:string, c:string", "[a, b, c]", "a", table1 );    
        createTable( table2RowCount, table2DupFactor, "a:int, d:string", "[a, d]", "a", table2 );

        String qs1 = "T1 = load '" + table1.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader('a, b, c', 'sorted');";
        System.out.println( "qs1: " + qs1 );
        String qs2 = "T2 = load '" + table2.toString() + "' USING org.apache.hadoop.zebra.pig.TableLoader('a, d', 'sorted');";
        System.out.println( "qs2: " + qs2 );

        pigServer.registerQuery( qs1 );
        pigServer.registerQuery( qs2 );

        String qs3 = "T3 = cogroup T1 by a, T2 by a USING 'merge';";
        pigServer.registerQuery( qs3 );

        org.apache.pig.impl.logicalLayer.schema.Schema schema = pigServer.dumpSchema( "T3" );
        Assert.assertEquals( "{group: int,T1: {(a: int,b: chararray,c: chararray)},T2: {(a: int,d: chararray)}}",
                schema.toString() );
        Iterator<Tuple> it = pigServer.openIterator( "T3" );
        int count = 0;
        int expectedCount = Math.max( table1RowCount/table1DupFactor, table2RowCount/table2DupFactor) + 1;
        int totalRowsInBag1 = 0;
        int totalRowsInBag2 = 0;
        while( it.hasNext() ) {
            Tuple result = it.next();
            totalRowsInBag1 += ( (DataBag)result.get( 1 ) ).size();
            totalRowsInBag2 += ( (DataBag)result.get( 2 ) ).size();
//            System.out.println( "tuple = " + result.toDelimitedString( "," ) );
            count++;
        }

        Assert.assertEquals( expectedCount, count );
        Assert.assertEquals(table1RowCount, totalRowsInBag1 );
        Assert.assertEquals(table2RowCount, totalRowsInBag2 );
    }

    public static void createTable(int rows, int step, String strSchema, String storage, String sortColumns, Path path)
    throws IOException {
        if( fs.exists(path) ) {
            BasicTable.drop(path, conf);
        }

        BasicTable.Writer writer = new BasicTable.Writer(path, strSchema, storage, sortColumns, null, conf);
        writer.finish();

        Schema schema = writer.getSchema();
        String colNames[] = schema.getColumns();
        Tuple tuple = TypesUtils.createTuple(schema);

        writer = new BasicTable.Writer(path, conf);
        TableInserter inserter = writer.getInserter( String.format("part-%06d", 1), true );
        for( int i = 1; i <= rows; ++i ) {
            BytesWritable key = new BytesWritable( String.format( "key%09d", i/step ).getBytes() );
            TypesUtils.resetTuple(tuple);
            tuple.set( 0,  i / step );
            for( int k = 1; k < tuple.size(); ++k ) {
                try {
                    tuple.set( k, new String( "col-" + colNames[k] + i * 10 ) );
                } catch (ExecException e) {
                    e.printStackTrace();
                }
            }
            inserter.insert(key, tuple);
        }
        inserter.close();

        writer = new BasicTable.Writer(path, conf);
        writer.close();
    }

}
