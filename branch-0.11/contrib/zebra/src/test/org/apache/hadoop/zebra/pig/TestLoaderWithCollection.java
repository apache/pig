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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.BaseTestCase;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

public class TestLoaderWithCollection extends BaseTestCase
{
    private static Path pathTable;

    @BeforeClass
    public static void setUp() throws Exception {
      init();
      pathTable = getTableFullPath("TestMapTableLoader");
      removeDir(pathTable);

        BasicTable.Writer writer = new BasicTable.Writer(pathTable,
                "c:collection(record(a:double))", "[c]", conf);
        Schema schema = writer.getSchema();
        Tuple tuple = TypesUtils.createTuple(schema);

        final int numsBatch = 10;
        final int numsInserters = 2;
        TableInserter[] inserters = new TableInserter[numsInserters];
        for (int i = 0; i < numsInserters; i++) {
            inserters[i] = writer.getInserter("ins" + i, false);
        }

        for (int b = 0; b < numsBatch; b++) {
            for (int i = 0; i < numsInserters; i++) {
                TypesUtils.resetTuple(tuple);

                DataBag bagColl = TypesUtils.createBag();
                Schema schColl = schema.getColumn(0).getSchema().getColumn(0).getSchema();
                Tuple tupColl1 = TypesUtils.createTuple(schColl);
                Tuple tupColl2 = TypesUtils.createTuple(schColl);
                tupColl1.set(0, 3.1415926);
                bagColl.add(tupColl1);
                tupColl2.set(0, 123.456789);
                bagColl.add(tupColl2);
                tuple.set(0, bagColl);

                inserters[i].insert(new BytesWritable(("key" + i).getBytes()), tuple);
            }
        }
        for (int i = 0; i < numsInserters; i++) {
            inserters[i].close();
        }
        writer.close();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        pigServer.shutdown();
    }

    @Test
    public void test() throws ExecException, IOException {
        String query = "records = LOAD '" + pathTable.toString()
        + "' USING org.apache.hadoop.zebra.pig.TableLoader('c');";
        System.out.println(query);
        pigServer.registerQuery(query);
        Iterator<Tuple> it = pigServer.openIterator("records");
        while (it.hasNext()) {
            Tuple cur = it.next();
            System.out.println(cur);
        }
    }
}
