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
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
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
    public static void setUp() throws ExecException {
        ps = new PigServer(ExecType.LOCAL);
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
}
