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

package org.apache.pig.newplan.logical.optimizer;

import static org.apache.pig.ExecType.LOCAL;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.pig.PigRunner;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.Util;
import org.junit.Test;

/**
 *
 * See: https://issues.apache.org/jira/browse/PIG-3020
 *
 */
public class TestSchemaResetter {

    @Test
    public void testSchemaResetter() throws IOException {
        new File("build/test/tmp/").mkdirs();
        Util.createLocalInputFile("build/test/tmp/TestSchemaResetter.pig", new String[] {
                "A = LOAD 'foo' AS (group:tuple(uid, dst_id));",
                "edges_both = FOREACH A GENERATE",
                "    group.uid AS src_id,",
                "    group.dst_id AS dst_id;",
                "both_counts = GROUP edges_both BY src_id;",
                "both_counts = FOREACH both_counts GENERATE",
                "    group AS src_id, SIZE(edges_both) AS size_both;",
                "",
                "edges_bq = FOREACH A GENERATE",
                "    group.uid AS src_id,",
                "    group.dst_id AS dst_id;",
                "bq_counts = GROUP edges_bq BY src_id;",
                "bq_counts = FOREACH bq_counts GENERATE",
                "    group AS src_id, SIZE(edges_bq) AS size_bq;",
                "",
                "per_user_set_sizes = JOIN bq_counts BY src_id LEFT OUTER, both_counts BY src_id;",
                "store per_user_set_sizes into  'foo';"
                });
        assertEquals(0, PigRunner.run(new String[] {"-x", "local", "-c", "build/test/tmp/TestSchemaResetter.pig" } , null).getReturnCode());
    }

    @Test
    public void testSchemaResetterExec() throws IOException {
        PigServer pigServer = new PigServer(LOCAL);
        Data data = Storage.resetData(pigServer);
        data.set("input",
                tuple(tuple("1", "2")),
                tuple(tuple("2", "3")),
                tuple(tuple("2", "4")));
        pigServer.registerQuery(
                "A = LOAD 'input' USING mock.Storage() AS (group:tuple(uid, dst_id));" +
                "edges_both = FOREACH A GENERATE" +
                "    group.uid AS src_id," +
                "    group.dst_id AS dst_id;" +
                "both_counts = GROUP edges_both BY src_id;" +
                "both_counts = FOREACH both_counts GENERATE" +
                "    group AS src_id, SIZE(edges_both) AS size_both;" +
                "edges_bq = FOREACH A GENERATE" +
                "    group.uid AS src_id," +
                "    group.dst_id AS dst_id;" +
                "bq_counts = GROUP edges_bq BY src_id;" +
                "bq_counts = FOREACH bq_counts GENERATE" +
                "    group AS src_id, SIZE(edges_bq) AS size_bq;" +
                "per_user_set_sizes = JOIN bq_counts BY src_id LEFT OUTER, both_counts BY src_id;" +
                "store per_user_set_sizes into 'output' USING mock.Storage();");
        List<Tuple> list = data.get("output");
        Collections.sort(list);
        assertEquals("list: "+list, 2, list.size());
        assertEquals("(1,1,1,1)", list.get(0).toString());
        assertEquals("(2,2,2,2)", list.get(1).toString());
    }
}
