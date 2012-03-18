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

import java.util.Iterator;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.AfterClass;
import org.junit.Test;

public class TestNestedForeach {
	static MiniCluster cluster = MiniCluster.buildCluster();

	private PigServer pig ;

	public TestNestedForeach() throws Throwable {
		pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties()) ;
	}

	Boolean[] nullFlags = new Boolean[]{ false, true };

	@AfterClass
	public static void oneTimeTearDown() throws Exception {
		cluster.shutDown();
	}

	@Test
	public void testNestedForeachProj() throws Exception {
		String[] input = {
				"1\t2",
				"2\t7",
				"1\t3"
		};

		Util.createInputFile(cluster, "table_nf_proj", input);

		pig.registerQuery("a = load 'table_nf_proj' as (a0:int, a1:int);\n");
		pig.registerQuery("b = group a by a0;\n");
		pig.registerQuery("c = foreach b { c1 = foreach a generate a1; generate c1; }\n");

		Iterator<Tuple> iter = pig.openIterator("c");
		String[] expected = new String[] {"({(2),(3)})", "({(7)})"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(pig.dumpSchema("c")));
    
	}

	@Test
	public void testNestedForeachExpression() throws Exception {
		String[] input = {
				"1\t2",
				"2\t7",
				"1\t3"
		};

		Util.createInputFile(cluster, "table_nf_expr", input);

		pig.registerQuery("a = load 'table_nf_expr' as (a0:int, a1:int);\n");
		pig.registerQuery("b = group a by a0;\n");
		pig.registerQuery("c = foreach b { c1 = foreach a generate 2 * a1; generate c1; }\n");

		Iterator<Tuple> iter = pig.openIterator("c");
		
        String[] expected = new String[] {"({(4),(6)})", "({(14)})"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(pig.dumpSchema("c")));
	}

	@Test
	public void testNestedForeachUDF() throws Exception {
		String[] input = {
				"1\thello",
				"2\tpig",
				"1\tworld"
		};

		Util.createInputFile(cluster, "table_nf_udf", input);

		pig.registerQuery("a = load 'table_nf_udf' as (a0:int, a1:chararray);\n");
		pig.registerQuery("b = group a by a0;\n");
		pig.registerQuery("c = foreach b { c1 = foreach a generate UPPER(a1); generate c1; }\n");

		Iterator<Tuple> iter = pig.openIterator("c");
		
        String[] expected = new String[] {"({(HELLO),(WORLD)})", "({(PIG)})"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(pig.dumpSchema("c")));
	}

	@Test
	public void testNestedForeachFlatten() throws Exception {
		String[] input = {
				"1\thello world pig",
				"2\thadoop world",
				"1\thello pig"
		};

		Util.createInputFile(cluster, "table_nf_flatten", input);

		pig.registerQuery("a = load 'table_nf_flatten' as (a0:int, a1:chararray);\n");
		pig.registerQuery("b = group a by a0;\n");
		pig.registerQuery("c = foreach b { c1 = foreach a generate FLATTEN(TOKENIZE(a1)); generate c1; }\n");

		Iterator<Tuple> iter = pig.openIterator("c");
		
        String[] expected = new String[] {"({(hello),(world),(pig),(hello),(pig)})", 
                "({(hadoop),(world)})"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(pig.dumpSchema("c")));
	}

	@Test
	public void testNestedForeachInnerFilter() throws Exception {
		String[] input = {
				"1\t2",
				"2\t7",
				"1\t3"
		};

		Util.createInputFile(cluster, "table_nf_filter", input);

		pig.registerQuery("a = load 'table_nf_filter' as (a0:int, a1:int);\n");
		pig.registerQuery("b = group a by a0;\n");
		pig.registerQuery("c = foreach b { " +
				" c1 = filter a by a1 >= 3; " +
				" c2 = foreach c1 generate a1; " +
				" generate c2; " +
		" }\n");

		Iterator<Tuple> iter = pig.openIterator("c");
		Tuple t = iter.next();
		Assert.assertTrue(t.toString().equals("({(3)})"));

		t = iter.next();
		Assert.assertTrue(t.toString().equals("({(7)})"));
	}

	@Test
	public void testNestedForeachInnerOrder() throws Exception {
		String[] input = {
				"1\t3",
				"2\t7",
				"1\t2"
		};

		Util.createInputFile(cluster, "table_nf_order", input);

		pig.registerQuery("a = load 'table_nf_order' as (a0:int, a1:int);\n");
		pig.registerQuery("b = group a by a0;\n");
		pig.registerQuery("c = foreach b { " +
				" c1 = order a by a1; " +
				" c2 = foreach c1 generate a1; " +
				" generate c2; " +
		" }\n");

		Iterator<Tuple> iter = pig.openIterator("c");
		Tuple t = iter.next();
		Assert.assertTrue(t.toString().equals("({(2),(3)})"));

		t = iter.next();
		Assert.assertTrue(t.toString().equals("({(7)})"));
	}
	
	// See PIG-2563
	@Test
    public void testNestedForeach() throws Exception {
        String[] input = {
                "1\t2\t3",
                "2\t5\t2"
        };

        Util.createInputFile(cluster, "table_nf_project", input);

        pig.registerQuery("A = load 'table_nf_project' as (a,b,c:chararray);");
        pig.registerQuery("B = GROUP A BY a;");
        pig.registerQuery("C = foreach B {tmp = A.a;generate A, tmp; };");
        pig.registerQuery("D = foreach C generate A.(a,b) as v;");
        Iterator<Tuple> iter = pig.openIterator("D");
        Tuple t = iter.next();
        Assert.assertTrue(t.toString().equals("({(1,2)})"));

        t = iter.next();
        Assert.assertTrue(t.toString().equals("({(2,5)})"));
    }
}
