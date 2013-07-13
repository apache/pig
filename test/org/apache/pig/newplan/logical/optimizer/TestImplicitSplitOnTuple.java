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


public class TestImplicitSplitOnTuple {

    @Test
    public void testImplicitSplitterOnTuple() throws IOException {
        PigServer pigServer = new PigServer(LOCAL);
        Data data = Storage.resetData(pigServer);
        data.set("input",
                tuple("1", "1001", "101"),
                tuple("1", "1002", "103"),
                tuple("1", "1003", "102"),
                tuple("1", "1004", "102"),
                tuple("2", "1005", "101"),
                tuple("2", "1003", "101"),
                tuple("2", "1002", "123"),
                tuple("3", "1042", "101"),
                tuple("3", "1005", "101"),
                tuple("3", "1002", "133"));

        pigServer.registerQuery(
                "inp = LOAD 'input' USING mock.Storage() AS (memberId:long, shopId:long, score:int);"+
                "tuplified = FOREACH inp GENERATE (memberId, shopId) AS tuplify, score;"+
                "D1 = FOREACH tuplified GENERATE tuplify.memberId as memberId, tuplify.shopId as shopId, score AS score;"+
                "D2 = FOREACH tuplified GENERATE tuplify.memberId as memberId, tuplify.shopId as shopId, score AS score;"+
                "J = JOIN D1 By shopId, D2 by shopId;"+
                "K = FOREACH J GENERATE D1::memberId AS member_id1, D2::memberId AS member_id2, D1::shopId as shop;"+
                "L = ORDER K by shop;"+
                "STORE L into 'output' using mock.Storage;");
        List<Tuple> list = data.get("output");
        assertEquals("list: "+list, 20, list.size());
        assertEquals("(1,1,1001)", list.get(0).toString());
        assertEquals("(1,1,1002)", list.get(1).toString());
        assertEquals("(1,2,1002)", list.get(2).toString());
        assertEquals("(1,3,1002)", list.get(3).toString());
        assertEquals("(2,1,1002)", list.get(4).toString());
        assertEquals("(2,2,1002)", list.get(5).toString());
        assertEquals("(2,3,1002)", list.get(6).toString());
        assertEquals("(3,1,1002)", list.get(7).toString());
        assertEquals("(3,2,1002)", list.get(8).toString());
        assertEquals("(3,3,1002)", list.get(9).toString());
        assertEquals("(1,1,1003)", list.get(10).toString());
        assertEquals("(1,2,1003)", list.get(11).toString());
        assertEquals("(2,1,1003)", list.get(12).toString());
        assertEquals("(2,2,1003)", list.get(13).toString());
        assertEquals("(1,1,1004)", list.get(14).toString());
        assertEquals("(2,2,1005)", list.get(15).toString());
        assertEquals("(2,3,1005)", list.get(16).toString());
        assertEquals("(3,2,1005)", list.get(17).toString());
        assertEquals("(3,3,1005)", list.get(18).toString());
        assertEquals("(3,3,1042)", list.get(19).toString());
    }

}
