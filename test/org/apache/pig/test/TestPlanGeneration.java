/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ExecType;
import org.apache.pig.Expression;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import static org.apache.pig.builtin.mock.Storage.*;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.parser.ParserException;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPlanGeneration {

    private static PigContext pc;
    private static PigServer ps;

    @BeforeClass
    public static void setUp() throws Exception {
        ps = new PigServer(Util.getLocalTestMode());
        pc = ps.getPigContext();
        pc.connect();
    }

    @Test
    public void testGenerateStar() throws Exception {
        String query = "a = load 'x';" +
                "b = foreach a generate *;" +
                "store b into '111';";

        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOForEach loForEach = (LOForEach)lp.getPredecessors(loStore).get(0);
        assertNull(loForEach.getSchema());
    }

    @Test
    public void testEmptyBagDereference() throws Exception {
        String query = "A = load 'x' as ( u:bag{} );" +
                "B = foreach A generate u.$100;" +
                "store B into '111';";

        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOForEach loForEach = (LOForEach)lp.getPredecessors(loStore).get(0);
        LogicalSchema schema = loForEach.getSchema();
        assertEquals(1, schema.size());
        LogicalFieldSchema bagFieldSchema = schema.getField(0);
        assertEquals(DataType.BAG, bagFieldSchema.type);
        LogicalFieldSchema tupleFieldSchema = bagFieldSchema.schema.getField(0);
        assertEquals(1, tupleFieldSchema.schema.size());
        assertEquals(DataType.BYTEARRAY, tupleFieldSchema.schema.getField(0).type);
    }

    @Test
    public void testEmptyTupleDereference() throws Exception {
        String query = "A = load 'x' as ( u:tuple() );" +
                "B = foreach A generate u.$100;" +
                "store B into '111';";

        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOForEach loForEach = (LOForEach)lp.getPredecessors(loStore).get(0);
        LogicalSchema schema = loForEach.getSchema();
        assertEquals(1, schema.size());
        assertEquals(DataType.BYTEARRAY, schema.getField(0).type);
    }

    @Test
    public void testEmptyBagInnerPlan() throws Exception {
        String query = "A = load 'x' as ( u:bag{} );" +
                "B = foreach A { B1 = filter u by $1==0; generate B1;};" +
                "store B into '111';";

        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOForEach loForEach = (LOForEach)lp.getPredecessors(loStore).get(0);
        LogicalSchema schema = loForEach.getSchema();
        assertEquals(1, schema.size());
        LogicalFieldSchema bagFieldSchema = schema.getField(0);
        assertEquals(DataType.BAG, bagFieldSchema.type);
        LogicalFieldSchema tupleFieldSchema = bagFieldSchema.schema.getField(0);
        assertNull(tupleFieldSchema.schema);
    }

    @Test
    public void testOrderByNullFieldSchema() throws Exception {
        String query = "A = load 'x';" +
                "B = order A by *;" +
                "store B into '111';";

        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOSort loSort = (LOSort)lp.getPredecessors(loStore).get(0);
        Operator sortPlanLeaf = loSort.getSortColPlans().get(0).getSources().get(0);
        LogicalFieldSchema sortPlanFS = ((LogicalExpression)sortPlanLeaf).getFieldSchema();
        assertNull(sortPlanFS);

        PhysicalPlan pp = Util.buildPhysicalPlanFromNewLP(lp, pc);
        POStore poStore = (POStore)pp.getLeaves().get(0);
        POSort poSort = (POSort)pp.getPredecessors(poStore).get(0);
        POProject poProject = (POProject)poSort.getSortPlans().get(0).getLeaves().get(0);
        assertEquals(DataType.TUPLE, poProject.getResultType());
    }

    @Test
    public void testGroupByNullFieldSchema() throws Exception {
        String query = "A = load 'x';" +
                "B = group A by *;" +
                "store B into '111';";

        LogicalPlan lp = Util.parseAndPreprocess(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        LOCogroup loCoGroup = (LOCogroup)lp.getPredecessors(loStore).get(0);
        LogicalFieldSchema groupFieldSchema = loCoGroup.getSchema().getField(0);
        assertEquals(DataType.TUPLE, groupFieldSchema.type);
        assertNull(groupFieldSchema.schema);
    }

    @Test
    public void testStoreAlias() throws Exception {
        String query = "A = load 'data' as (a0, a1);" +
                "B = filter A by a0 > 1;" +
                "store B into 'output';";

        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);
        LOStore loStore = (LOStore)lp.getSinks().get(0);
        assertEquals("B", loStore.getAlias());

        PhysicalPlan pp = Util.buildPhysicalPlanFromNewLP(lp, pc);
        POStore poStore = (POStore)pp.getLeaves().get(0);
        assertEquals("B", poStore.getAlias());

        MROperPlan mrp = Util.buildMRPlanWithOptimizer(pp, pc);
        MapReduceOper mrOper = mrp.getLeaves().get(0);
        poStore = (POStore)mrOper.mapPlan.getLeaves().get(0);
        assertEquals("B", poStore.getAlias());
    }

    // See PIG-2119
    @Test
    public void testDanglingNestedNode() throws Exception {
        String query = "a = load 'b.txt' AS (id:chararray, num:int); " +
                "b = group a by id;" +
                "c = foreach b {" +
                "  d = order a by num DESC;" +
                "  n = COUNT(a);" +
                "  e = limit d 1;" +
                "  generate n;" +
                "};";

        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);
    }

    public static class SchemaLoader extends PigStorage implements LoadMetadata {

        Schema schema;

        public SchemaLoader(String schemaString) throws ParserException {
            schema = Utils.getSchemaFromString(schemaString);
        }

        @Override
        public ResourceSchema getSchema(String location, Job job)
                throws IOException {
            return new ResourceSchema(schema);
        }

        @Override
        public ResourceStatistics getStatistics(String location, Job job)
                throws IOException {
            return null;
        }

        @Override
        public String[] getPartitionKeys(String location, Job job)
                throws IOException {
            return null;
        }

        @Override
        public void setPartitionFilter(Expression partitionFilter)
                throws IOException {
        }
    }

    @Test
    public void testLoaderWithSchema() throws Exception {
        String query = "a = load 'foo' using " + SchemaLoader.class.getName()
                + "('name,age,gpa');\n"
                + "b = filter a by age==20;"
                + "store b into 'output';";
        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);

        LOLoad loLoad = (LOLoad)lp.getSources().get(0);
        LOFilter loFilter = (LOFilter)lp.getSuccessors(loLoad).get(0);
        LOStore loStore = (LOStore)lp.getSuccessors(loFilter).get(0);

        assertNull(lp.getSuccessors(loStore));
    }

    public static class PartitionedLoader extends PigStorage implements LoadMetadata {

        Schema schema;
        String[] partCols;
        static Expression partFilter = null;

        public PartitionedLoader(String schemaString, String commaSepPartitionCols)
                throws ParserException {
            schema = Utils.getSchemaFromString(schemaString);
            partCols = commaSepPartitionCols.split(",");
        }

        @Override
        public ResourceSchema getSchema(String location, Job job)
                throws IOException {
            return new ResourceSchema(schema);
        }

        @Override
        public ResourceStatistics getStatistics(String location,
                Job job) throws IOException {
            return null;
        }

        @Override
        public void setPartitionFilter(Expression partitionFilter)
                throws IOException {
            partFilter = partitionFilter;
        }

        @Override
        public String[] getPartitionKeys(String location, Job job)
                throws IOException {
            return partCols;
        }

        public Expression getPartFilter() {
            return partFilter;
        }

    }

    @Test
    // See PIG-2339
    public void testPartitionFilterOptimizer() throws Exception {
        String query = "a = load 'foo' using " + PartitionedLoader.class.getName() +
                "('name:chararray, dt:chararray', 'dt');\n" +
                "b = filter a by dt=='2011';\n" +
                "store b into 'output';";

        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);

        LOLoad loLoad = (LOLoad)lp.getSources().get(0);
        LOStore loStore = (LOStore)lp.getSuccessors(loLoad).get(0);
        assertNotNull(((PartitionedLoader)loLoad.getLoadFunc()).getPartFilter());
        assertEquals("b", loStore.getAlias());
    }

    @Test
    // See PIG-2315
    public void testForEachWithCast1() throws Exception {
        // A cast ForEach is inserted to take care of the user schema
        String query = "A = load 'foo' as (a, b:int);\n" +
                "B = foreach A generate a as a0:chararray, b as b:int;\n" +
                "store B into 'output';";

        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);

        LOLoad loLoad = (LOLoad)lp.getSources().get(0);
        LOForEach loForEach1 = (LOForEach)lp.getSuccessors(loLoad).get(0);
        LOForEach loForEach2 = (LOForEach)lp.getSuccessors(loForEach1).get(0);
        // before a0 is typecasted to chararray, it should be bytearray
        assertEquals(DataType.BYTEARRAY, loForEach1.getSchema().getField(0).type);
        // type of b should stay as int
        assertEquals(DataType.INTEGER, loForEach1.getSchema().getField(1).type);
        assertEquals("B", loForEach2.getAlias());
        LOGenerate generate = (LOGenerate)loForEach2.getInnerPlan().getSinks().get(0);
        CastExpression cast = (CastExpression)generate.getOutputPlans().get(0).getSources().get(0);
        Assert.assertTrue(cast.getType()==DataType.CHARARRAY);
        assertEquals(loForEach2.getSchema().getField(0).alias, "a0");
        Assert.assertTrue(lp.getSuccessors(loForEach2).get(0) instanceof LOStore);
    }

    @Test
    // See PIG-2315
    public void testForEachWithCast2() throws Exception {
        // No additional cast ForEach will be inserted, but schema should match
        String query = "A = load 'foo' as (a, b);\n" +
                "B = foreach A generate (chararray)a as a0:chararray;\n" +
                "store B into 'output';";

        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);

        LOLoad loLoad = (LOLoad)lp.getSources().get(0);
        LOForEach loForEach = (LOForEach)lp.getSuccessors(loLoad).get(0);
        assertEquals(loForEach.getSchema().getField(0).alias, "a0");
        Assert.assertTrue(lp.getSuccessors(loForEach).get(0) instanceof LOStore);
    }

    @Test
    // See PIG-2315
    public void testForEachWithCast3() throws Exception {
        // No additional cast ForEach will be inserted, but schema should match
        String query = "A = load 'foo' as (a, b);\n" +
                "B = foreach A generate (chararray)a as a0:int;\n" +
                "store B into 'output';";

        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);

        LOLoad loLoad = (LOLoad)lp.getSources().get(0);
        LOForEach loForEach1 = (LOForEach)lp.getSuccessors(loLoad).get(0);
        LOGenerate generate1 = (LOGenerate)loForEach1.getInnerPlan().getSinks().get(0);
        CastExpression cast1 = (CastExpression)generate1.getOutputPlans().get(0).getSources().get(0);
        Assert.assertTrue(cast1.getType()==DataType.CHARARRAY);
        //before a0 is typecasted to int, it should be chararray
        Assert.assertEquals(DataType.CHARARRAY, loForEach1.getSchema().getField(0).type);
        LOForEach loForEach2 = (LOForEach)lp.getSuccessors(loForEach1).get(0);
        LOGenerate generate2 = (LOGenerate)loForEach2.getInnerPlan().getSinks().get(0);
        CastExpression cast2 = (CastExpression)generate2.getOutputPlans().get(0).getSources().get(0);
        Assert.assertTrue(cast2.getType()==DataType.INTEGER);
        Assert.assertTrue(lp.getSuccessors(loForEach2).get(0) instanceof LOStore);
    }

    @Test
    // See PIG-2315
    public void testForEachWithCast4() throws Exception {
        // No additional cast ForEach will be inserted
        String query = "a = load 'foo' as (nb1:bag{}, nb2:chararray);\n" +
                "b = foreach a generate flatten(nb1) as (year, name), nb2;\n" +
                "store b into 'output';";

        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);

        LOLoad loLoad = (LOLoad)lp.getSources().get(0);
        LOForEach loForEach = (LOForEach)lp.getSuccessors(loLoad).get(0);
        Assert.assertTrue(lp.getSuccessors(loForEach).get(0) instanceof LOStore);
    }

    @Test
    // See PIG-2315
    public void testForEachWithCast5() throws Exception {
        // cast ForEach will be inserted
        String query = "a = load 'foo' as (nb1:bag{}, nb2:chararray);\n" +
                "b = foreach a generate flatten(nb1) as (year, name:chararray), nb2 as nb2:chararray;\n" +
                "store b into 'output';";

        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);

        LOLoad loLoad = (LOLoad)lp.getSources().get(0);
        LOForEach loForEach1 = (LOForEach)lp.getSuccessors(loLoad).get(0);
        // flattened "name" field should be bytearray before typecasted to  chararray
        Assert.assertEquals(DataType.BYTEARRAY, loForEach1.getSchema().getField(1).type);
        LOForEach loForEach2 = (LOForEach)lp.getSuccessors(loForEach1).get(0);
        LOGenerate generate = (LOGenerate)loForEach2.getInnerPlan().getSinks().get(0);
        Assert.assertTrue(generate.getOutputPlans().get(0).getSources().get(0) instanceof ProjectExpression);
        CastExpression cast = (CastExpression)generate.getOutputPlans().get(1).getSources().get(0);
        Assert.assertTrue(cast.getType()==DataType.CHARARRAY);
        Assert.assertTrue(generate.getOutputPlans().get(2).getSources().get(0) instanceof ProjectExpression);
    }

    @Test
    // See PIG-2315
    public void testForEachWithCast6() throws Exception {
        // no cast ForEach will be inserted
        String query = "a = load 'foo' as (nb1:bag{(year,name)}, nb2);\n" +
                "b = foreach a generate flatten(nb1) as (year, name2), nb2;\n" +
                "store b into 'output';";

        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);

        LOLoad loLoad = (LOLoad)lp.getSources().get(0);
        LOForEach loForEach = (LOForEach)lp.getSuccessors(loLoad).get(0);
        assertEquals(loForEach.getSchema().getField(1).alias, "name2");
        Assert.assertTrue(lp.getSuccessors(loForEach).get(0) instanceof LOStore);
    }

    @Test
    // See PIG-2315
    public void testForEachWithCast7() throws Exception {
        // no cast ForEach will be inserted, since we don't know the size of outputs
        // in first inner plan
        String query = "a = load 'foo' as (nb1:bag{}, nb2:bag{});\n" +
                "b = foreach a generate flatten(nb1), flatten(nb2) as (year, name);\n" +
                "store b into 'output';";

        LogicalPlan lp = Util.parse(query, pc);
        Util.optimizeNewLP(lp);

        LOLoad loLoad = (LOLoad)lp.getSources().get(0);
        LOForEach loForEach = (LOForEach)lp.getSuccessors(loLoad).get(0);
        Assert.assertTrue(lp.getSuccessors(loForEach).get(0) instanceof LOStore);
    }

    @Test
    // See PIG-2315
    public void testAsType1() throws Exception {
        Data data = Storage.resetData(ps);
        data.set("input", tuple(0.1), tuple(1.2), tuple(2.3));

        String query =
            "A = load 'input' USING mock.Storage() as (a1:double);\n"
            + "B = FOREACH A GENERATE a1 as (a2:int);\n"
            + "store B into 'out' using mock.Storage;" ;

        Util.registerMultiLineQuery(ps, query);
        List<Tuple> list = data.get("out");
        // Without PIG-2315, this failed with (0.1), (1.2), (2.3)
        List<Tuple> expectedRes =
                Util.getTuplesFromConstantTupleStrings(
                        new String[] {"(0)", "(1)", "(2)"});
        Util.checkQueryOutputsAfterSort(list, expectedRes);
    }

    @Test
    // See PIG-2315
    public void testAsType2() throws Exception {
        Data data = Storage.resetData(ps);
        data.set("input", tuple("a"), tuple("b"), tuple("c"));

        String query =
            "A = load 'input' USING mock.Storage(); \n"
            + "A2 = FOREACH A GENERATE 12345 as (a2:chararray); \n"
            + "B = load 'input' USING mock.Storage(); \n"
            + "B2 = FOREACH A GENERATE '12345' as (b2:chararray); \n"
            + "C = union A2, B2;\n"
            + "D = distinct C;\n"
            + "store D into 'out' using mock.Storage;" ;

        Util.registerMultiLineQuery(ps, query);
        List<Tuple> list = data.get("out");
        // Without PIG-2315, this produced TWO 12345.
        // One by chararray and another by int.
        List<Tuple> expectedRes =
                Util.getTuplesFromConstantTupleStrings(
                        new String[] {"('12345')"});
        Util.checkQueryOutputsAfterSort(list, expectedRes);
    }

    @Test
    // See PIG-4933
    public void testAsWithByteArrayCast() throws Exception {
        Data data = Storage.resetData(ps);
	    data.set("input_testAsWithByteArrayCast", "t1:(f1:bytearray, f2:bytearray), f3:chararray",
				tuple(tuple(1,5), "a"),
				tuple(tuple(2,4), "b"),
				tuple(tuple(3,3), "c") );

        String query =
            "A = load 'input_testAsWithByteArrayCast' USING mock.Storage();\n"
            + "B = FOREACH A GENERATE t1 as (t2:(newf1, newf2:float)), f3;"
            + "store B into 'out' using mock.Storage;" ;

        // This will call typecast of (bytearray,float) on a tuple
        // bytearray2bytearray should be no-op.
        // Without pig-4933 patch on POCast,
        // this typecast was producing empty results

        Util.registerMultiLineQuery(ps, query);
        List<Tuple> list = data.get("out");
        String[] expectedRes =
                        new String[] {"((1,5.0),a)","((2,4.0),b)","((3,3.0),c)"};
        for( int i=0; i < list.size(); i++ ) {
            Assert.assertEquals(expectedRes[i], list.get(i).toString());
        }
    }
}
