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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;

import java.io.IOException;
import java.util.List;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

public class TestRank1 {
    private static TupleFactory tf = TupleFactory.getInstance();
    private static PigServer pigServer;
    private Data data;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(Util.getLocalTestMode());

        data = resetData(pigServer);
        data.set("test01", tuple("A", 1, "N"), tuple("B", 2, "N"),
                tuple("C", 3, "M"), tuple("D", 4, "P"), tuple("E", 4, "Q"),
                tuple("E", 4, "Q"), tuple("F", 8, "Q"), tuple("F", 7, "Q"),
                tuple("F", 8, "T"), tuple("F", 8, "Q"), tuple("G", 10, "V"));

        data.set(
                "test02",
                tuple("Michael", "Blythe", 1, 1, 1, 1, 4557045.046, 98027),
                tuple("Linda", "Mitchell", 2, 1, 1, 1, 5200475.231, 98027),
                tuple("Jillian", "Carson", 3, 1, 1, 1, 3857163.633, 98027),
                tuple("Garrett", "Vargas", 4, 1, 1, 1, 1764938.986, 98027),
                tuple("Tsvi", "Reiter", 5, 1, 1, 2, 2811012.715, 98027),
                tuple("Shu", "Ito", 6, 6, 2, 2, 3018725.486, 98055),
                tuple("Jose", "Saraiva", 7, 6, 2, 2, 3189356.247, 98055),
                tuple("David", "Campbell", 8, 6, 2, 3, 3587378.426, 98055),
                tuple("Tete", "Mensa-Annan", 9, 6, 2, 3, 1931620.184, 98055),
                tuple("Lynn", "Tsoflias", 10, 6, 2, 3, 1758385.926, 98055),
                tuple("Rachel", "Valdez", 11, 6, 2, 4, 2241204.042, 98055),
                tuple("Jae", "Pak", 12, 6, 2, 4, 5015682.375, 98055),
                tuple("Ranjit", "Varkey Chudukatil", 13, 6, 2, 4,
                        3827950.238, 98055));
    }

    @Test
    public void testRank01RowNumber() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);
        List<Tuple> expected = Util.getTuplesFromConstantTupleStrings(new String[]{
                "(1L,'A',1,'N')",
                "(2L,'B',2,'N')",
                "(3L,'C',3,'M')",
                "(4L,'D',4,'P')",
                "(5L,'E',4,'Q')",
                "(6L,'E',4,'Q')",
                "(7L,'F',8,'Q')",
                "(8L,'F',7,'Q')",
                "(9L,'F',8,'T')",
                "(10L,'F',8,'Q')",
                "(11L,'G',10,'V')"
        });
        Util.checkQueryOutputsAfterSort(data.get("result"), expected);
    }

    @Test
    public void testRank02RowNumber() throws IOException {
        String query = "A = LOAD 'test02' USING mock.Storage() AS (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "B = rank A;"
            + "store B into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);
        List<Tuple> expected = Util.getTuplesFromConstantTupleStrings(new String[]{
                "(1L,'Michael', 'Blythe', 1,1, 1, 1, 4557045.046, 98027)",
                "(2L,'Linda','Mitchell', 2, 1, 1, 1, 5200475.231, 98027)",
                "(3L,'Jillian', 'Carson', 3,1, 1, 1, 3857163.633, 98027)",
                "(4L,'Garrett','Vargas', 4, 1, 1, 1, 1764938.986, 98027)",
                "(5L,'Tsvi', 'Reiter',5, 1, 1, 2, 2811012.715, 98027)",
                "(6L,'Shu', 'Ito', 6,6, 2, 2, 3018725.486, 98055)",
                "(7L,'Jose', 'Saraiva',7, 6, 2, 2, 3189356.247, 98055)",
                "(8L,'David','Campbell', 8, 6, 2, 3, 3587378.426, 98055)",
                "(9L,'Tete', 'Mensa-Annan',9, 6, 2, 3, 1931620.184, 98055)",
                "(10L, 'Lynn','Tsoflias', 10, 6, 2, 3, 1758385.926, 98055)",
                "(11L, 'Rachel', 'Valdez', 11,6, 2, 4, 2241204.042, 98055)",
                "(12L, 'Jae', 'Pak', 12,6, 2, 4, 5015682.375, 98055)",
                "(13L, 'Ranjit','Varkey Chudukatil', 13, 6, 2, 4, 3827950.238,98055)"
        });
        Util.checkQueryOutputsAfterSort(data.get("result"), expected);
    }

    @Test
    public void testRank01RankBy() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f3;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);
        List<Tuple> expected = Util.getTuplesFromConstantTupleStrings(new String[]{
                "(1L,'C',3,'M')",
                "(2L,'A',1,'N')",
                "(2L,'B',2,'N')",
                "(4L,'D',4,'P')",
                "(5L,'E',4,'Q')",
                "(5L,'E',4,'Q')",
                "(5L,'F',8,'Q')",
                "(5L,'F',7,'Q')",
                "(5L,'F',8,'Q')",
                "(10L,'F',8,'T')",
                "(11L,'G',10,'V')"
        });
        Util.checkQueryOutputsAfterSort(data.get("result"), expected);
    }

    @Test
    public void testRank02RankBy() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f2 ASC;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);
        List<Tuple> expected = Util.getTuplesFromConstantTupleStrings(new String[]{
                "(1L,'A',1,'N')",
                "(2L,'B',2,'N')",
                "(3L,'C',3,'M')",
                "(4L,'D',4,'P')",
                "(4L,'E',4,'Q')",
                "(4L,'E',4,'Q')",
                "(7L,'F',7,'Q')",
                "(8L,'F',8,'Q')",
                "(8L,'F',8,'Q')",
                "(8L,'F',8,'T')",
                "(11L,'G',10,'V')"
        });
        Util.checkQueryOutputsAfterSort(data.get("result"), expected);
    }

    @Test
    public void testRank03RankBy() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f1 DESC;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);
        List<Tuple> expected = Util.getTuplesFromConstantTupleStrings(new String[]{
                "(1L,'G',10,'V')",
                "(2L,'F',8,'T')",
                "(2L,'F',8,'Q')",
                "(2L,'F',8,'Q')",
                "(2L,'F',7,'Q')",
                "(6L,'E',4,'Q')",
                "(6L,'E',4,'Q')",
                "(8L,'D',4,'P')",
                "(9L,'C',3,'M')",
                "(10L,'B',2,'N')",
                "(11L,'A',1,'N')"
        });
        Util.checkQueryOutputsAfterSort(data.get("result"), expected);
    }

    @Test
    public void testRank04RankBy() throws IOException {
        String query = "A = LOAD 'test02' USING mock.Storage() AS (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "C = rank A by postalcode;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);
        List<Tuple> expected = Util.getTuplesFromConstantTupleStrings(new String[]{
                "(1L,'Michael','Blythe',1,1,1,1,4557045.046,98027)",
                "(1L,'Linda','Mitchell',2,1,1,1,5200475.231,98027)",
                "(1L,'Jillian','Carson',3,1,1,1,3857163.633,98027)",
                "(1L,'Garrett','Vargas',4,1,1,1,1764938.986,98027)",
                "(1L,'Tsvi','Reiter',5,1,1,2,2811012.715,98027)",
                "(6L,'Shu','Ito',6,6,2,2,3018725.486,98055)",
                "(6L,'Jose','Saraiva',7,6,2,2,3189356.247,98055)",
                "(6L,'David','Campbell',8,6,2,3,3587378.426,98055)",
                "(6L,'Tete','Mensa-Annan',9,6,2,3,1931620.184,98055)",
                "(6L,'Lynn','Tsoflias',10,6,2,3,1758385.926,98055)",
                "(6L,'Rachel','Valdez',11,6,2,4,2241204.042,98055)",
                "(6L,'Jae','Pak',12,6,2,4,5015682.375,98055)",
                "(6L,'Ranjit','Varkey Chudukatil',13,6,2,4,3827950.238,98055)",
        });
        Util.checkQueryOutputsAfterSort(data.get("result"), expected);
    }

    @Test
    public void testRank05RankBy() throws IOException {
        String query = "A = LOAD 'test02' USING mock.Storage() AS (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "C = rank A by *;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);
        List<Tuple> expected = Util.getTuplesFromConstantTupleStrings(new String[]{
                "(1L,'David','Campbell',8,6,2,3,3587378.426,98055)",
                "(2L,'Garrett','Vargas',4,1,1,1,1764938.986,98027)",
                "(3L,'Jae','Pak',12,6,2,4,5015682.375,98055)",
                "(4L,'Jillian','Carson',3,1,1,1,3857163.633,98027)",
                "(5L,'Jose','Saraiva',7,6,2,2,3189356.247,98055)",
                "(6L,'Linda','Mitchell',2,1,1,1,5200475.231,98027)",
                "(7L,'Lynn','Tsoflias',10,6,2,3,1758385.926,98055)",
                "(8L,'Michael','Blythe',1,1,1,1,4557045.046,98027)",
                "(9L,'Rachel','Valdez',11,6,2,4,2241204.042,98055)",
                "(10L,'Ranjit','Varkey Chudukatil',13,6,2,4,3827950.238,98055)",
                "(11L,'Shu','Ito',6,6,2,2,3018725.486,98055)",
                "(12L,'Tete','Mensa-Annan',9,6,2,3,1931620.184,98055)",
                "(13L,'Tsvi','Reiter',5,1,1,2,2811012.715,98027)"
        });
        Util.checkQueryOutputsAfterSort(data.get("result"), expected);
    }

    @Test
    public void testRank06RankBy() throws IOException {
        String query = "A = LOAD 'test02' USING mock.Storage() AS (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "C = rank A by $0..$2;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);
        List<Tuple> expected = Util.getTuplesFromConstantTupleStrings(new String[]{
                "(1L,'David','Campbell',8,6,2,3,3587378.426,98055)",
                "(2L,'Garrett','Vargas',4,1,1,1,1764938.986,98027)",
                "(3L,'Jae','Pak',12,6,2,4,5015682.375,98055)",
                "(4L,'Jillian','Carson',3,1,1,1,3857163.633,98027)",
                "(5L,'Jose','Saraiva',7,6,2,2,3189356.247,98055)",
                "(6L,'Linda','Mitchell',2,1,1,1,5200475.231,98027)",
                "(7L,'Lynn','Tsoflias',10,6,2,3,1758385.926,98055)",
                "(8L,'Michael','Blythe',1,1,1,1,4557045.046,98027)",
                "(9L,'Rachel','Valdez',11,6,2,4,2241204.042,98055)",
                "(10L,'Ranjit','Varkey Chudukatil',13,6,2,4,3827950.238,98055)",
                "(11L,'Shu','Ito',6,6,2,2,3018725.486,98055)",
                "(12L,'Tete','Mensa-Annan',9,6,2,3,1931620.184,98055)",
                "(13L,'Tsvi','Reiter',5,1,1,2,2811012.715,98027)"
        });
        Util.checkQueryOutputsAfterSort(data.get("result"), expected);
    }

    @Test
    public void testRank07RankBy() throws IOException {
        String query = "A = LOAD 'test01' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);"
            + "C = rank A by f1..f3;"
            + "store C into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);
        List<Tuple> expected = Util.getTuplesFromConstantTupleStrings(new String[]{
                "(1L,'A',1,'N')",
                "(2L,'B',2,'N')",
                "(3L,'C',3,'M')",
                "(4L,'D',4,'P')",
                "(5L,'E',4,'Q')",
                "(5L,'E',4,'Q')",
                "(7L,'F',7,'Q')",
                "(8L,'F',8,'Q')",
                "(8L,'F',8,'Q')",
                "(10L,'F',8,'T')",
                "(11L,'G',10,'V')"
        });
        Util.checkQueryOutputsAfterSort(data.get("result"), expected);
    }

}
