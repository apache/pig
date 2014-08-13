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
import static org.apache.pig.newplan.logical.relational.LOTestHelper.newLOLoad;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.LogicalRelationalNodeValidator;
import org.apache.pig.parser.QueryParser;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.validator.BlackAndWhitelistFilter;
import org.apache.pig.validator.BlackAndWhitelistValidator;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * Contains tests for {@link BlackAndWhitelistValidator} and
 * {@link BlackAndWhitelistFilter}
 * 
 */
public class TestBlackAndWhitelistValidator {
    private PigContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = new PigContext(ExecType.LOCAL, new Properties());
        ctx.connect();
    }

    /**
     * Tests the blacklist filter. We blacklist "set" and make sure this test
     * throws a {@link FrontendException}
     * 
     * @throws Exception
     */
    @Test
    public void testBlacklist() throws Exception {
        try {
            ctx.getProperties().setProperty(PigConfiguration.PIG_BLACKLIST, "set");
            PigServer pigServer = new PigServer(ctx);
            Data data = resetData(pigServer);

            data.set("foo", tuple("a", 1, "b"), tuple("b", 2, "c"),
                    tuple("c", 3, "d"));

            StringBuilder script = new StringBuilder();
            script.append("set io.sort.mb 1000;")
                    .append("A = LOAD 'foo' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);")
                    .append("B = order A by f1,f2,f3 DESC;")
                    .append("STORE B INTO 'bar' USING mock.Storage();");

            pigServer.registerScript(IOUtils.toInputStream(script));
            fail();
        } catch (Exception e) {
            Util.assertExceptionAndMessage(FrontendException.class, e,
                    "SET command is not permitted. ");
        }
    }

    /**
     * Tests {@link BlackAndWhitelistValidator}. The logical plan generated
     * contains a filter, and the test must throw a {@link FrontendException} as
     * we set "filter" in the blacklist
     * 
     * @throws Exception
     */
    @Test
    public void testValidator() throws Exception {
        try {
            // disabling filter
            ctx.getProperties().setProperty(PigConfiguration.PIG_BLACKLIST, "filter");

            LogicalPlan plan = generateLogicalPlan("foo", "bar", ctx.getDfs());

            LogicalRelationalNodeValidator executor = new BlackAndWhitelistValidator(ctx, plan);
            executor.validate();
            fail();
        } catch (Exception e) {
            Util.assertExceptionAndMessage(FrontendException.class, e,
                    "filter is disabled. ");
        }
    }

    /**
     * This test must pass as we allow load, store, filter to be a part of the
     * whitelist. The logical plan being checked in this test contains these
     * operators only.
     * 
     * @throws Exception
     */
    @Test
    public void testWhitelist1() throws Exception {
        ctx.getProperties().setProperty(PigConfiguration.PIG_WHITELIST,
                "load, store,filter");
        LogicalPlan plan = generateLogicalPlan("foo", "bar", ctx.getDfs());

        LogicalRelationalNodeValidator executor = new BlackAndWhitelistValidator(ctx, plan);
        executor.validate();
    }

    @Test
    public void testWhitelist2() throws Exception {
        try {
            // only load and store are allowed. Having a filter in the logical
            // plan must cause the script to fail
            ctx.getProperties().setProperty(PigConfiguration.PIG_WHITELIST, "load, store");
            LogicalPlan plan = generateLogicalPlan("foo", "bar", ctx.getDfs());

            LogicalRelationalNodeValidator executor = new BlackAndWhitelistValidator(ctx, plan);
            executor.validate();
            fail();
        } catch (Exception e) {
            Util.assertExceptionAndMessage(FrontendException.class, e,
                    "filter is disabled. ");
        }
    }

    /**
     * If there is a conflict between blacklist and whitelist contents, the
     * validator or filter must throw an {@link IllegalStateException}.
     * 
     * @throws Exception
     */
    @Test
    public void testBlackAndWhitelist() throws Exception {
        try {
            ctx.getProperties().setProperty(PigConfiguration.PIG_WHITELIST, "load, store, filter");
            ctx.getProperties().setProperty(PigConfiguration.PIG_BLACKLIST, "filter");
            LogicalPlan plan = generateLogicalPlan("foo", "bar", ctx.getDfs());

            LogicalRelationalNodeValidator executor = new BlackAndWhitelistValidator(ctx, plan);
            executor.validate();
            fail();
        } catch (Exception e) {
            Util.assertExceptionAndMessage(IllegalStateException.class, e,
                    "Conflict between whitelist and blacklist. 'filter' appears in both.");
        }
    }

    /**
     * This is to test the script fails when used with {@link PigServer}, which
     * uses {@link QueryParser} and not the {@link GruntParser}
     */
    @Test(expected = FrontendException.class)
    public void testBlacklistWithPigServer() throws Exception {
        ctx.getProperties().setProperty(PigConfiguration.PIG_BLACKLIST, "order");
        PigServer pigServer = new PigServer(ctx);
        Data data = resetData(pigServer);

        data.set("foo", tuple("a", 1, "b"), tuple("b", 2, "c"),
                tuple("c", 3, "d"));

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);");
        pigServer.registerQuery("B = order A by f1,f2,f3 DESC;");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");
    }

    /**
     * Test listStatus fails if its disallowed via the blacklist
     */
    @Test
    public void testBlacklistCmdWithPigServer() throws Exception {
        try {
            ctx.getProperties().setProperty(PigConfiguration.PIG_BLACKLIST, "ls");
            PigServer pigServer = new PigServer(ctx);
            pigServer.listPaths("foo");
            fail();
        } catch (Exception e) {
            Util.assertExceptionAndMessage(FrontendException.class, e,
                    "LS command is not permitted. ");
        }
    }

    /**
     * Test deleteFile fails if its disallowed via the blacklist
     */
    @Test(expected = FrontendException.class)
    public void testBlacklistRemoveWithPigServer() throws Exception {
        ctx.getProperties().setProperty(PigConfiguration.PIG_BLACKLIST, "rm");
        PigServer pigServer = new PigServer(ctx);

        pigServer.deleteFile("foo");
    }

    /**
     * Test mkdirs fails if its disallowed via the blacklist
     */
    @Test(expected = FrontendException.class)
    public void testBlacklistMkdirWithPigServer() throws Exception {
        ctx.getProperties().setProperty(PigConfiguration.PIG_BLACKLIST, "mkdir");
        PigServer pigServer = new PigServer(ctx);

        pigServer.mkdirs("foo");
    }

    /**
     * This is to test the script fails when used with {@link PigServer}, which
     * uses {@link QueryParser} and not the {@link GruntParser}
     */
    @Test(expected = FrontendException.class)
    public void testWhitelistWithPigServer() throws Exception {
        ctx.getProperties().setProperty(PigConfiguration.PIG_WHITELIST, "load");
        PigServer pigServer = new PigServer(ctx);
        Data data = resetData(pigServer);

        data.set("foo", tuple("a", 1, "b"), tuple("b", 2, "c"),
                tuple("c", 3, "d"));

        pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage() AS (f1:chararray,f2:int,f3:chararray);");
        pigServer.registerQuery("B = order A by f1,f2,f3 DESC;");
        pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");
    }

    /**
     * 
     * Generate a {@link LogicalPlan} containing a Load, Filter and Store
     * operators
     * 
     * @param inputFile
     * @param outputFile
     * @param dfs
     * @return
     * @throws Exception
     */
    private LogicalPlan generateLogicalPlan(String inputFile,
            String outputFile, DataStorage dfs) throws Exception {
        LogicalPlan plan = new LogicalPlan();
        FileSpec filespec1 = new FileSpec(generateTmpFile(inputFile).getAbsolutePath(), new FuncSpec("org.apache.pig.builtin.PigStorage"));
        FileSpec filespec2 = new FileSpec(generateTmpFile(outputFile).getAbsolutePath(), new FuncSpec("org.apache.pig.builtin.PigStorage"));
        LOLoad load = newLOLoad(filespec1, null, plan, ConfigurationUtil.toConfiguration(dfs.getConfiguration()));
        LOStore store = new LOStore(plan, filespec2, (StoreFuncInterface) PigContext.instantiateFuncFromSpec(filespec2.getFuncSpec()), null);

        LOFilter filter = new LOFilter(plan);

        plan.add(load);
        plan.add(store);
        plan.add(filter);

        plan.connect(load, filter);
        plan.connect(filter, store);

        return plan;
    }

    private File generateTmpFile(String filename) throws Exception {
        return Util.createTempFileDelOnExit(filename, ".txt");
    }
}
