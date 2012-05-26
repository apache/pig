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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.newplan.Operator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestCubeOperator {
    private static PigServer pigServer;
    private static TupleFactory tf = TupleFactory.getInstance();
    private Data data;

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
	pigServer = new PigServer("local");

	data = resetData(pigServer);
	data.set("input", 
		tuple("dog", "miami", 12),
		tuple("cat", "miami", 18),
		tuple("turtle", "tampa", 4),
		tuple("dog", "tampa", 14),
		tuple("cat", "naples", 9),
		tuple("dog", "naples", 5),
		tuple("turtle", "naples", 1));

	data.set("input1", 
		tuple("u1,men,green,mango"),
		tuple("u2,men,red,mango"),
		tuple("u3,men,green,apple"),
		tuple("u4,women,red,mango"),
		tuple("u6,women,green,mango"),
		tuple("u7,men,red,apple"),
		tuple("u8,men,green,mango"),
		tuple("u9,women,red,apple"),
		tuple("u10,women,green,apple"),
		tuple("u11,men,red,apple"),
		tuple("u12,women,green,mango"));
    }

    @AfterClass
    public static void oneTimeTearDown() throws IOException {
    }

    @Test
    public void testCubeBasic() throws IOException {
	// basic correctness test
	String query =
		"a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);" +
			"b = cube a by (x,y);" +
			"c = foreach b generate flatten(group) as (type,location), COUNT(cube) as count, SUM(cube.z) as total;" +
			"store c into 'output' using mock.Storage();";
	Util.registerMultiLineQuery(pigServer, query);

	Set<Tuple> expected = ImmutableSet.of(
		tf.newTuple(ImmutableList.of("cat", "miami", (long)1, (long)18)),
		tf.newTuple(ImmutableList.of("cat", "naples", (long)1, (long)9)),
		tf.newTuple(ImmutableList.of("cat", "NULL", (long)2, (long)27)),
		tf.newTuple(ImmutableList.of("dog", "miami", (long)1, (long)12)),
		tf.newTuple(ImmutableList.of("dog", "tampa", (long)1, (long)14)),
		tf.newTuple(ImmutableList.of("dog", "naples", (long)1, (long)5)),
		tf.newTuple(ImmutableList.of("dog", "NULL", (long)3, (long)31)),
		tf.newTuple(ImmutableList.of("turtle", "tampa", (long)1, (long)4)),
		tf.newTuple(ImmutableList.of("turtle", "naples", (long)1, (long)1)),
		tf.newTuple(ImmutableList.of("turtle", "NULL", (long)2, (long)5)),
		tf.newTuple(ImmutableList.of("NULL", "miami", (long)2, (long)30)),
		tf.newTuple(ImmutableList.of("NULL", "tampa", (long)2, (long)18)),
		tf.newTuple(ImmutableList.of("NULL", "naples", (long)3, (long)15)),
		tf.newTuple(ImmutableList.of("NULL", "NULL", (long)7, (long)63))
		);

	List<Tuple> out = data.get("output");
	for( Tuple tup : out ) {
	    assertTrue(expected+" contains "+tup, expected.contains(tup));
	}

    }

    @Test
    public void testCubeMultipleIAliases() throws IOException {
	// test for input alias to cube being assigned multiple times
	String query = 
		"a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);" +
			"a = load 'input' USING mock.Storage() as (x,y:chararray,z:long);" +
			"a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);" +
			"b = cube a by (x,y);" + 
			"c = foreach b generate flatten(group) as (type,location), COUNT(cube) as count, SUM(cube.z) as total;" +
			"store c into 'output' using mock.Storage();";

	Util.registerMultiLineQuery(pigServer, query);

	Set<Tuple> expected = ImmutableSet.of(
		tf.newTuple(ImmutableList.of("cat", "miami", (long)1, (long)18)),
		tf.newTuple(ImmutableList.of("cat", "naples", (long)1, (long)9)),
		tf.newTuple(ImmutableList.of("cat", "NULL", (long)2, (long)27)),
		tf.newTuple(ImmutableList.of("dog", "miami", (long)1, (long)12)),
		tf.newTuple(ImmutableList.of("dog", "tampa", (long)1, (long)14)),
		tf.newTuple(ImmutableList.of("dog", "naples", (long)1, (long)5)),
		tf.newTuple(ImmutableList.of("dog", "NULL", (long)3, (long)31)),
		tf.newTuple(ImmutableList.of("turtle", "tampa", (long)1, (long)4)),
		tf.newTuple(ImmutableList.of("turtle", "naples", (long)1, (long)1)),
		tf.newTuple(ImmutableList.of("turtle", "NULL", (long)2, (long)5)),
		tf.newTuple(ImmutableList.of("NULL", "miami", (long)2, (long)30)),
		tf.newTuple(ImmutableList.of("NULL", "tampa", (long)2, (long)18)),
		tf.newTuple(ImmutableList.of("NULL", "naples", (long)3, (long)15)),
		tf.newTuple(ImmutableList.of("NULL", "NULL", (long)7, (long)63))
		);

	List<Tuple> out = data.get("output");
	for( Tuple tup : out ) {
	    assertTrue(expected+" contains "+tup, expected.contains(tup));
	}

    }

    @Test
    public void testCubeAfterForeach() throws IOException {
	// test for foreach projection before cube operator
	String query = 
		"a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);" +
			"b = foreach a generate x as type,y as location,z as number;" +
			"c = cube b by (type,location);" + 
			"d = foreach c generate flatten(group) as (type,location), COUNT(cube) as count, SUM(cube.number) as total;" +
			"store d into 'output' using mock.Storage();";

	Util.registerMultiLineQuery(pigServer, query);

	Set<Tuple> expected = ImmutableSet.of(
		tf.newTuple(ImmutableList.of("cat", "miami", (long)1, (long)18)),
		tf.newTuple(ImmutableList.of("cat", "naples", (long)1, (long)9)),
		tf.newTuple(ImmutableList.of("cat", "NULL", (long)2, (long)27)),
		tf.newTuple(ImmutableList.of("dog", "miami", (long)1, (long)12)),
		tf.newTuple(ImmutableList.of("dog", "tampa", (long)1, (long)14)),
		tf.newTuple(ImmutableList.of("dog", "naples", (long)1, (long)5)),
		tf.newTuple(ImmutableList.of("dog", "NULL", (long)3, (long)31)),
		tf.newTuple(ImmutableList.of("turtle", "tampa", (long)1, (long)4)),
		tf.newTuple(ImmutableList.of("turtle", "naples", (long)1, (long)1)),
		tf.newTuple(ImmutableList.of("turtle", "NULL", (long)2, (long)5)),
		tf.newTuple(ImmutableList.of("NULL", "miami", (long)2, (long)30)),
		tf.newTuple(ImmutableList.of("NULL", "tampa", (long)2, (long)18)),
		tf.newTuple(ImmutableList.of("NULL", "naples", (long)3, (long)15)),
		tf.newTuple(ImmutableList.of("NULL", "NULL", (long)7, (long)63))
		);

	List<Tuple> out = data.get("output");
	for( Tuple tup : out ) {
	    assertTrue(expected+" contains "+tup, expected.contains(tup));
	}

    }

    @Test
    public void testCubeAfterLimit() throws IOException {
	// test for limit operator before cube operator
	String query = 
		"a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);" +
			"b = limit a 2;" +
			"c = cube b by (x,y);" + 
			"d = foreach c generate flatten(group) as (x,y), SUM(cube.z) as total;" +
			"store d into 'output' using mock.Storage();";

	Util.registerMultiLineQuery(pigServer, query);

	Set<Tuple> expected = ImmutableSet.of(
		tf.newTuple(ImmutableList.of("cat", "miami", (long)18)),
		tf.newTuple(ImmutableList.of("cat", "NULL", (long)18)),
		tf.newTuple(ImmutableList.of("dog", "miami", (long)12)),
		tf.newTuple(ImmutableList.of("dog", "NULL", (long)12)),
		tf.newTuple(ImmutableList.of("NULL", "miami", (long)30)),
		tf.newTuple(ImmutableList.of("NULL", "NULL", (long)30))
		);

	List<Tuple> out = data.get("output");
	for( Tuple tup : out ) {
	    assertTrue(expected+" contains "+tup, expected.contains(tup));
	}

    }

    @Test
    public void testCubeWithStar() throws IOException {
	// test for * (all) dimensions in cube operator
	String query = 
		"a = load 'input' USING mock.Storage() as (x:chararray,y:chararray);" +
			"b = foreach a generate x as type,y as location;" +
			"c = cube b by (*);" + 
			"d = foreach c generate flatten(group) as (type,location), COUNT(cube) as count;" +
			"store d into 'output' using mock.Storage();";

	Util.registerMultiLineQuery(pigServer, query);

	Set<Tuple> expected = ImmutableSet.of(
		tf.newTuple(ImmutableList.of("cat", "miami", (long)1)),
		tf.newTuple(ImmutableList.of("cat", "naples", (long)1)),
		tf.newTuple(ImmutableList.of("cat", "NULL", (long)2)),
		tf.newTuple(ImmutableList.of("dog", "miami", (long)1)),
		tf.newTuple(ImmutableList.of("dog", "tampa", (long)1)),
		tf.newTuple(ImmutableList.of("dog", "naples", (long)1)),
		tf.newTuple(ImmutableList.of("dog", "NULL", (long)3)),
		tf.newTuple(ImmutableList.of("turtle", "tampa", (long)1)),
		tf.newTuple(ImmutableList.of("turtle", "naples", (long)1)),
		tf.newTuple(ImmutableList.of("turtle", "NULL", (long)2)),
		tf.newTuple(ImmutableList.of("NULL", "miami", (long)2)),
		tf.newTuple(ImmutableList.of("NULL", "tampa", (long)2)),
		tf.newTuple(ImmutableList.of("NULL", "naples", (long)3)),
		tf.newTuple(ImmutableList.of("NULL", "NULL", (long)7))
		);

	List<Tuple> out = data.get("output");
	for( Tuple tup : out ) {
	    assertTrue(expected+" contains "+tup, expected.contains(tup));
	}

    }

    @Test
    public void testCubeWithRange() throws IOException {
	// test for range projection of dimensions in cube operator
	String query = 
		"a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);" +
			"b = foreach a generate x as type,y as location, z as number;" +
			"c = cube b by ($0..$1);" + 
			"d = foreach c generate flatten(group) as (type,location), COUNT(cube) as count, SUM(cube.number) as total;" +
			"store d into 'output' using mock.Storage();";

	Util.registerMultiLineQuery(pigServer, query);

	Set<Tuple> expected = ImmutableSet.of(
		tf.newTuple(ImmutableList.of("cat", "miami", (long)1, (long)18)),
		tf.newTuple(ImmutableList.of("cat", "naples", (long)1, (long)9)),
		tf.newTuple(ImmutableList.of("cat", "NULL", (long)2, (long)27)),
		tf.newTuple(ImmutableList.of("dog", "miami", (long)1, (long)12)),
		tf.newTuple(ImmutableList.of("dog", "tampa", (long)1, (long)14)),
		tf.newTuple(ImmutableList.of("dog", "naples", (long)1, (long)5)),
		tf.newTuple(ImmutableList.of("dog", "NULL", (long)3, (long)31)),
		tf.newTuple(ImmutableList.of("turtle", "tampa", (long)1, (long)4)),
		tf.newTuple(ImmutableList.of("turtle", "naples", (long)1, (long)1)),
		tf.newTuple(ImmutableList.of("turtle", "NULL", (long)2, (long)5)),
		tf.newTuple(ImmutableList.of("NULL", "miami", (long)2, (long)30)),
		tf.newTuple(ImmutableList.of("NULL", "tampa", (long)2, (long)18)),
		tf.newTuple(ImmutableList.of("NULL", "naples", (long)3, (long)15)),
		tf.newTuple(ImmutableList.of("NULL", "NULL", (long)7, (long)63))
		);

	List<Tuple> out = data.get("output");
	for( Tuple tup : out ) {
	    assertTrue(expected+" contains "+tup, expected.contains(tup));
	}

    }

    @Test
    public void testCubeDuplicateDimensions() throws IOException {
	// test for cube operator with duplicate dimensions
	String query = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
		+ "b = foreach a generate x as type,y as location, z as number;"
		+ "c = cube b by ($0..$1,$0..$1);"
		+ "d = foreach c generate flatten(group), COUNT(cube) as count, SUM(cube.number) as total;"
		+ "store d into 'output' using mock.Storage();";

	try {
	    Util.registerMultiLineQuery(pigServer, query);
	    pigServer.openIterator("d");
	} catch (FrontendException e) {
	    // FEException with 'duplicate dimensions detected' message is throw
	    return;
	}

	Assert.fail("Expected to throw an exception when duplicate dimensions are detected!");

    }

    @Test
    public void testCubeAfterFilter() throws IOException {
	// test for filtering before cube operator
	String query = 
		"a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);" +
			"b = filter a by x == 'dog';" +
			"c = cube b by (x,y);" + 
			"d = foreach c generate flatten(group), COUNT(cube) as count, SUM(cube.z) as total;" +
			"store d into 'output' using mock.Storage();";

	Util.registerMultiLineQuery(pigServer, query);
	// Iterator<Tuple> it = pigServer.openIterator("d");

	Set<Tuple> expected = ImmutableSet.of(
		tf.newTuple(ImmutableList.of("dog", "miami", (long)1, (long)12)),
		tf.newTuple(ImmutableList.of("dog", "tampa", (long)1, (long)14)),
		tf.newTuple(ImmutableList.of("dog", "naples", (long)1, (long)5)),
		tf.newTuple(ImmutableList.of("dog", "NULL", (long)3, (long)31)),
		tf.newTuple(ImmutableList.of("NULL", "miami", (long)1, (long)12)),
		tf.newTuple(ImmutableList.of("NULL", "tampa", (long)1, (long)14)),
		tf.newTuple(ImmutableList.of("NULL", "naples", (long)1, (long)5)),
		tf.newTuple(ImmutableList.of("NULL", "NULL", (long)3, (long)31))
		);

	List<Tuple> out = data.get("output");
	for( Tuple tup : out ) {
	    assertTrue(expected+" contains "+tup, expected.contains(tup));
	}

    }

    @Test
    public void testCubeAfterOrder() throws IOException {
	// test for ordering before cube operator
	String query = 
		"a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);" +
			"b = order a by $2;" +
			"c = cube b by (x,y);" + 
			"d = foreach c generate flatten(group), COUNT(cube) as count, SUM(cube.z) as total;" +
			"store d into 'output' using mock.Storage();";

	Util.registerMultiLineQuery(pigServer, query);

	Set<Tuple> expected = ImmutableSet.of(
		tf.newTuple(ImmutableList.of("cat", "miami", (long)1, (long)18)),
		tf.newTuple(ImmutableList.of("cat", "naples", (long)1, (long)9)),
		tf.newTuple(ImmutableList.of("cat", "NULL", (long)2, (long)27)),
		tf.newTuple(ImmutableList.of("dog", "miami", (long)1, (long)12)),
		tf.newTuple(ImmutableList.of("dog", "tampa", (long)1, (long)14)),
		tf.newTuple(ImmutableList.of("dog", "naples", (long)1, (long)5)),
		tf.newTuple(ImmutableList.of("dog", "NULL", (long)3, (long)31)),
		tf.newTuple(ImmutableList.of("turtle", "tampa", (long)1, (long)4)),
		tf.newTuple(ImmutableList.of("turtle", "naples", (long)1, (long)1)),
		tf.newTuple(ImmutableList.of("turtle", "NULL", (long)2, (long)5)),
		tf.newTuple(ImmutableList.of("NULL", "miami", (long)2, (long)30)),
		tf.newTuple(ImmutableList.of("NULL", "tampa", (long)2, (long)18)),
		tf.newTuple(ImmutableList.of("NULL", "naples", (long)3, (long)15)),
		tf.newTuple(ImmutableList.of("NULL", "NULL", (long)7, (long)63))
		);

	List<Tuple> out = data.get("output");
	for( Tuple tup : out ) {
	    assertTrue(expected+" contains "+tup, expected.contains(tup));
	}
    }

    @Test
    public void testCubeAfterJoin() throws IOException {
	// test for cubing on joined relations
	String query = 
		"a = load 'input1' USING mock.Storage() as (a1:chararray,b1,c1,d1); " +
			"b = load 'input' USING mock.Storage() as (a2,b2,c2:long,d2:chararray);" +
			"c = join a by a1, b by d2;" +
			"d = cube c by ($4,$5);" + 
			"e = foreach d generate flatten(group), COUNT(cube) as count, SUM(cube.c2) as total;" +
			"store e into 'output' using mock.Storage();";

	Util.registerMultiLineQuery(pigServer, query);

	Set<Tuple> expected = ImmutableSet.of(
		tf.newTuple(ImmutableList.of("cat", "miami", (long)1, (long)18)),
		tf.newTuple(ImmutableList.of("cat", "NULL", (long)1, (long)18)),
		tf.newTuple(ImmutableList.of("dog", "miami", (long)1, (long)12)),
		tf.newTuple(ImmutableList.of("dog", "tampa", (long)1, (long)14)),
		tf.newTuple(ImmutableList.of("dog", "NULL", (long)2, (long)26)),
		tf.newTuple(ImmutableList.of("turtle", "tampa", (long)1, (long)4)),
		tf.newTuple(ImmutableList.of("turtle", "naples", (long)1, (long)1)),
		tf.newTuple(ImmutableList.of("turtle", "NULL", (long)2, (long)5)),
		tf.newTuple(ImmutableList.of("NULL", "miami", (long)2, (long)30)),
		tf.newTuple(ImmutableList.of("NULL", "tampa", (long)2, (long)18)),
		tf.newTuple(ImmutableList.of("NULL", "naples", (long)1, (long)1)),
		tf.newTuple(ImmutableList.of("NULL", "NULL", (long)5, (long)49))
		);

	List<Tuple> out = data.get("output");
	for( Tuple tup : out ) {
	    assertTrue(expected+" contains "+tup, expected.contains(tup));
	}
    }

    @Test
    public void testCubeAfterCogroup() throws IOException {
	// test for cubing on co-grouped relation
	String query = 
		"a = load 'input1' USING mock.Storage() as (a1:chararray,b1,c1,d1); " +
			"b = load 'input' USING mock.Storage() as (a2,b2,c2:long,d2:chararray);" +
			"c = cogroup a by a1, b by d2;" +
			"d = foreach c generate flatten(a), flatten(b);" +
			"e = cube d by (a2,b2);" +
			"f = foreach e generate flatten(group), COUNT(cube) as count, SUM(cube.c2) as total;" +
			"store f into 'output' using mock.Storage();";

	Util.registerMultiLineQuery(pigServer, query);

	Set<Tuple> expected = ImmutableSet.of(
		tf.newTuple(ImmutableList.of("cat", "miami", (long)1, (long)18)),
		tf.newTuple(ImmutableList.of("cat", "NULL", (long)1, (long)18)),
		tf.newTuple(ImmutableList.of("dog", "miami", (long)1, (long)12)),
		tf.newTuple(ImmutableList.of("dog", "tampa", (long)1, (long)14)),
		tf.newTuple(ImmutableList.of("dog", "NULL", (long)2, (long)26)),
		tf.newTuple(ImmutableList.of("turtle", "tampa", (long)1, (long)4)),
		tf.newTuple(ImmutableList.of("turtle", "naples", (long)1, (long)1)),
		tf.newTuple(ImmutableList.of("turtle", "NULL", (long)2, (long)5)),
		tf.newTuple(ImmutableList.of("NULL", "miami", (long)2, (long)30)),
		tf.newTuple(ImmutableList.of("NULL", "tampa", (long)2, (long)18)),
		tf.newTuple(ImmutableList.of("NULL", "naples", (long)1, (long)1)),
		tf.newTuple(ImmutableList.of("NULL", "NULL", (long)5, (long)49))
		);

	List<Tuple> out = data.get("output");
	for( Tuple tup : out ) {
	    assertTrue(expected+" contains "+tup, expected.contains(tup));
	}
    }

    @Test
    public void testIllustrate() throws IOException {
	// test for illustrate
	String query = 
		"a = load 'input' USING mock.Storage() as (a1:chararray,b1:chararray,c1:long); " +
			"b = cube a by (a1,b1);";

	Util.registerMultiLineQuery(pigServer, query);
	Map<Operator, DataBag> examples = pigServer.getExamples("b");
	assertTrue(examples != null);
    }

    @Test
    public void testExplain() throws IOException {
	// test for explain
	String query = 
		"a = load 'input' USING mock.Storage() as (a1:chararray,b1:chararray,c1:long); " +
			"b = cube a by (a1,b1);";

	Util.registerMultiLineQuery(pigServer, query);
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	PrintStream ps = new PrintStream(baos);
	pigServer.explain("b", ps);
	assertTrue(baos.toString().contains("CubeDimensions('NULL')"));
    }

    @Test
    public void testDescribe() throws IOException {
	// test for describe
	String query = 
		"a = load 'input' USING mock.Storage() as (a1:chararray,b1:chararray,c1:long); " +
			"b = cube a by (a1,b1);";

	Util.registerMultiLineQuery(pigServer, query);
	Schema sch = pigServer.dumpSchema("b");
	for(String alias : sch.getAliases()) {
	    if(alias.compareTo("cube") == 0) {
		assertTrue(alias.contains("cube"));
	    }
	}
    }
}
