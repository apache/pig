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


import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigRunner;
import org.apache.pig.impl.PigContext;
import org.apache.pig.parser.NonProjectExpressionException;
import org.apache.pig.parser.ParserTestingUtils;
import org.apache.pig.parser.ParserUtil;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMacroExpansion {

    private static final MiniCluster cluster = MiniCluster.buildCluster();
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        cluster.shutDown();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test 
    public void firstTest() throws Throwable {
        String macro = "define group_and_count (A,group_key) returns B {\n" +
            "    D = group $A by $group_key partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel 50;\n" +
            "    $B = foreach D generate group, COUNT($A);\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user);\n" +
            "delta = group_and_count (alpha, age);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "macro_group_and_count_D_0 = group alpha by (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 50;\n" +
            "gamma = foreach macro_group_and_count_D_0 generate group, COUNT(alpha);\n\n" +
            "macro_group_and_count_D_1 = group alpha by (age) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 50;\n" +
            "delta = foreach macro_group_and_count_D_1 generate group, COUNT(alpha);\n\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void distinctTest() throws Throwable {
        String macro = "define group_and_count (A,group_key, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 23);\n" +
            "delta = group_and_count (alpha, age, 32);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected = 
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "gamma = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n\n" +
            "delta = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 32;\n\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        Assert.assertEquals(expected, s);
    }   
    
    @Test
    public void limitTest() throws Throwable {
        String macro = "define group_and_count (A,group_key, size) returns B {\n" +
            "    $B = limit $A $size;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 20);\n" +
            "delta = group_and_count (alpha, age, 39);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "gamma = limit alpha 20;\n\n" +
            "delta = limit alpha 39;\n\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        Assert.assertEquals(expected, s);
    }   
    
    @Test
    public void sampleTest() throws Throwable {
        String macro = "define group_and_count (A, rate) returns B {\n" +
            "    $B = sample $A $rate;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, 0.01);\n" +
            "delta = group_and_count (alpha, 0.002);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected = 
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "gamma = sample alpha 0.01;\n\n" +
            "delta = sample alpha 0.002;\n\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void orderbyTest() throws Throwable {
        String macro = "define group_and_count (A,f1,f2) returns B {\n" +
            "    $B = ORDER $A BY $f1 ASC, $f2 DESC PARALLEL 3;\n" +
            "    C = ORDER $A BY * ASC PARALLEL 3;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, age);\n" +
            "delta = group_and_count (alpha, age, zip);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "gamma = ORDER alpha BY user ASC, age DESC PARALLEL 3;\n" +
            "macro_group_and_count_C_0 = ORDER alpha BY * ASC PARALLEL 3;\n\n" +
            "delta = ORDER alpha BY age ASC, zip DESC PARALLEL 3;\n" +
            "macro_group_and_count_C_1 = ORDER alpha BY * ASC PARALLEL 3;\n\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void crossTest() throws Throwable {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    $B = CROSS $A, $C partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "gamma = group_and_count (alpha, beta);\n" +
            "delta = group_and_count (beta, alpha);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "gamma = CROSS alpha, beta partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n\n" +
            "delta = CROSS beta, alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void loadTest() throws Throwable {
        String macro = "define group_and_count (path) returns B {\n" +
            "    D = load 'myfile.txt' using PigStorage('\t') AS (name:chararray, age:int, gpa:float);\n" +   
            "    $B = load '$path' using PigStorage('\t') AS (F:tuple(f1:int,f2:int,f3:int),T:tuple(t1:chararray,t2:int));\n" +
            "    E = load 'myfile.txt' using org.apache.pig.builtin.PigStorage('\t') AS (B:bag{T:tuple(t1:int, t2:int, t3:int)});\n" + 
            "    F = load 'myfile.txt' using org.apache.pig.builtin.PigStorage('\t') AS (T1:tuple(f1:int, f2:int), B:bag{T2:tuple(t1:float,t2:float)}, M:map[]);\n" + 
            "};\n";
        
        String script = 
            "gamma = group_and_count (myfile);\n" +
            "delta = group_and_count (mydir);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
                
        String expected =
            "\nmacro_group_and_count_D_0 = load 'myfile.txt' USING PigStorage('\t') AS (name:chararray, age:int, gpa:float);\n" +
            "gamma = load 'myfile' USING PigStorage('\t') AS (F:(f1:int, f2:int, f3:int), T:(t1:chararray, t2:int));\n" +
            "macro_group_and_count_E_0 = load 'myfile.txt' USING org.apache.pig.builtin.PigStorage('\t') AS B:bag{T:(t1:int, t2:int, t3:int)};\n" +
            "macro_group_and_count_F_0 = load 'myfile.txt' USING org.apache.pig.builtin.PigStorage('\t') AS (T1:(f1:int, f2:int), B:bag{T:(t1:float, t2:float)}, M:map[]);\n\n" +
            "macro_group_and_count_D_1 = load 'myfile.txt' USING PigStorage('\t') AS (name:chararray, age:int, gpa:float);\n" +
            "delta = load 'mydir' USING PigStorage('\t') AS (F:(f1:int, f2:int, f3:int), T:(t1:chararray, t2:int));\n" +
            "macro_group_and_count_E_1 = load 'myfile.txt' USING org.apache.pig.builtin.PigStorage('\t') AS B:bag{T:(t1:int, t2:int, t3:int)};\n" +
            "macro_group_and_count_F_1 = load 'myfile.txt' USING org.apache.pig.builtin.PigStorage('\t') AS (T1:(f1:int, f2:int), B:bag{T:(t1:float, t2:float)}, M:map[]);\n\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void storeTest() throws Throwable {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    $B = CROSS $A, $C partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "    STORE $A INTO 'myoutput' USING PigStorage ('*');\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "gamma = group_and_count (alpha, beta);\n" +
            "delta = group_and_count (beta, alpha);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "gamma = CROSS alpha, beta partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "STORE alpha INTO 'myoutput' USING PigStorage('*');\n\n" +
            "delta = CROSS beta, alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "STORE beta INTO 'myoutput' USING PigStorage('*');\n\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void streamTest() throws Throwable {
        String macro = "define group_and_count (A) returns B {\n" +
            "    $B = STREAM $A THROUGH `stream.pl -n 5`;\n" +
            "    DEFINE mycmd `stream.pl -n 5`;\n" +
            "    $B = STREAM $A THROUGH mycmd;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "gamma = STREAM alpha THROUGH `stream.pl -n 5`;\n" +
            "DEFINE mycmd `stream.pl -n 5`;\n" +
            "gamma = STREAM alpha THROUGH mycmd;\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test 
    public void defineTest() throws Throwable {
        String macro = "define group_and_count (A) returns B {\n" +
            "    DEFINE CMD `perl PigStreaming.pl - nameMap` input(stdin using PigStreaming(',')) output(stdout using PigStreaming(','));\n" +            
            "    DEFINE mycmd `stream_cmd input file.dat`;\n" +
            "    DEFINE Z `stream.pl` stderr('<dir>' limit 100);\n" +
            "    $B = STREAM $A THROUGH CMD;\n" +
            "    D = STREAM $A THROUGH mycmd;\n" +
            "    F = STREAM $A THROUGH Z;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "DEFINE CMD `perl PigStreaming.pl - nameMap` input(stdin USING PigStreaming(',')) output (stdout USING PigStreaming(','));\n" +
            "DEFINE mycmd `stream_cmd input file.dat`;\n" +
            "DEFINE Z `stream.pl` stderr ('<dir>' LIMIT 100);\n" +
            "gamma = STREAM alpha THROUGH CMD;\n" +
            "macro_group_and_count_D_0 = STREAM alpha THROUGH mycmd;\n" +
            "macro_group_and_count_F_0 = STREAM alpha THROUGH Z;\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test 
    public void defineTest2() throws Throwable {
        String macro = "define group_and_count (A) returns B {\n" +
            "    DEFINE CMD `stream.pl data.gz` SHIP('/work/stream.pl') CACHE('/input/data.gz#data.gz');\n" +
            "    $B = STREAM $A THROUGH CMD;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "DEFINE CMD `stream.pl data.gz` SHIP ( '/work/stream.pl') CACHE ( '/input/data.gz#data.gz');\n" +
            "gamma = STREAM alpha THROUGH CMD;\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void groupTest() throws Throwable {
        String macro = "define group_and_count (A,group_key) returns B {\n" +
            "    D = group $A by $group_key parallel 50;\n" +
            "    $B = foreach D generate group, COUNT($A);\n" +
            "    X = GROUP $A BY $group_key USING 'collected';\n" +
            "    Y = GROUP $A BY ($group_key, age) USING 'merge';\n" +
            "    Z = GROUP $A ALL;" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "macro_group_and_count_D_0 = group alpha by (user) parallel 50;\n" +
            "gamma = foreach macro_group_and_count_D_0 generate group, COUNT(alpha);\n" +
            "macro_group_and_count_X_0 = GROUP alpha BY (user) USING 'collected';\n" +
            "macro_group_and_count_Y_0 = GROUP alpha BY (user, age) USING 'merge';\n" +
            "macro_group_and_count_Z_0 = GROUP alpha ALL;\n\n" +
            "store gamma into 'byuser';\n";
        
        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void cogroupTest() throws Throwable {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    D = cogroup $A by user, $C by user parallel 50;\n" +
            "    $B = cogroup $A by user inner, $C by user inner parallel 50;\n" +
            "    Y = COGROUP $A BY (user, age), $C by (user, view) USING 'merge';\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "gamma = group_and_count (alpha, beta);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "macro_group_and_count_D_0 = cogroup alpha by (user), beta by (user) parallel 50;\n" +
            "gamma = cogroup alpha by (user) inner, beta by (user) inner parallel 50;\n" +
            "macro_group_and_count_Y_0 = COGROUP alpha BY (user, age), beta by (user, view) USING 'merge';\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void unionTest() throws Throwable {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    D = union $A, $C;\n" +
            "    $B = union onschema $A, $C;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "gamma = group_and_count (alpha, beta);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "macro_group_and_count_D_0 = union alpha, beta;\n" +
            "gamma = union onschema alpha, beta;\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void splitTest() throws Throwable {
        String macro = "define group_and_count (A,key) returns B {\n" +
            "    SPLIT $A INTO $B IF $key<7, Y IF $key==5, Z IF ($key<6 OR $key>6);\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, age);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
             "\nalpha = load 'users' as (user, age, zip);\n" +
            "SPLIT alpha INTO gamma IF age < 7, Y IF age == 5, Z IF (age < 6) OR (age > 6);\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void mapreduceTest() throws Throwable {
        String macro = "define group_and_count (A) returns B {\n" +
            "    $B = MAPREDUCE 'wordcount.jar' STORE $A INTO 'inputDir' LOAD 'outputDir' " + 
            "AS (word:chararray, count: int) `org.myorg.WordCount inputDir outputDir`;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
             "\nalpha = load 'users' as (user, age, zip);\n" +
            "gamma = MAPREDUCE 'wordcount.jar' STORE alpha INTO 'inputDir' LOAD 'outputDir' AS (word:chararray, count:int) `org.myorg.WordCount inputDir outputDir`;\n\n" +
            "store gamma into 'byuser';\n";
        
        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void joinTest() throws Throwable {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    $B = JOIN $A BY user, $C BY user using 'replicated' partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "    $B = JOIN $A BY $0, $C BY $1 using 'skewed' parallel 5;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = JOIN alpha BY (user), beta BY (user) USING 'replicated' partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "gamma = JOIN alpha BY ($0), beta BY ($1) USING 'skewed' parallel 5;\n\n" +
            "store gamma into 'byuser';\n";
        
        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void multiOutputsTest() throws Throwable {
        String macro = "define group_and_count (A,C) returns B, D {\n" +
            "    $B = JOIN $A BY user, $C BY user using 'replicated' partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "    $D = JOIN $A BY $0, $C BY $1 using 'skewed' parallel 5;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma, sigma = group_and_count (alpha,beta);\n" +
            "store gamma into 'byuser';\n" +
            "store sigma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = JOIN alpha BY (user), beta BY (user) USING 'replicated' partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "sigma = JOIN alpha BY ($0), beta BY ($1) USING 'skewed' parallel 5;\n\n" +
            "store gamma into 'byuser';\n" +
            "store sigma into 'byuser';\n";
        
        Assert.assertEquals(expected, s);
    }
    
    @Test 
    public void outerTest() throws Throwable {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    $B = JOIN $A BY user right outer, $C BY user partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "    D = JOIN $A BY user LEFT, $C BY user;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = JOIN alpha BY (user) right outer, beta BY (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "macro_group_and_count_D_0 = JOIN alpha BY (user) LEFT, beta BY (user);\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void filterTest() throws Throwable {
        String macro = "define group_and_count (A) returns B {\n" +
            "    $B = FILTER $A BY ($1 == 8) OR (NOT ($0+$2 > $1));\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "gamma = FILTER alpha BY (($1 == 8) OR ( NOT ($0 + $2 > $1)));\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test 
    public void foreachTest() throws Throwable {
        String macro = "define group_and_count (A) returns B {\n" +
            "    $B = foreach $A generate $0, $2;\n" +
            "    C = group $A by $0;\n" +
            "    X = FOREACH C GENERATE group, SUM ($A.$1);\n" +
            "    Y = FOREACH C GENERATE group, FLATTEN($A);\n" +
            "    Z = FOREACH C GENERATE FLATTEN($A.($1, $2)), FLATTEN($A.age);\n" +
            "    X = FOREACH $A GENERATE $1+$2 AS f1:int;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age:int, zip:int);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age:int, zip:int);\n" +
            "gamma = foreach alpha generate $0, $2;\n" +
            "macro_group_and_count_C_0 = group alpha by ($0);\n" +
            "macro_group_and_count_X_0 = FOREACH macro_group_and_count_C_0 GENERATE group, SUM(alpha.($1));\n" +
            "macro_group_and_count_Y_0 = FOREACH macro_group_and_count_C_0 GENERATE group, FLATTEN(alpha) ;\n" +
            "macro_group_and_count_Z_0 = FOREACH macro_group_and_count_C_0 GENERATE FLATTEN(alpha.($1, $2)) , FLATTEN(alpha.(age)) ;\n" +
            "macro_group_and_count_X_0 = FOREACH alpha GENERATE $1 + $2 AS f1:int;\n\n" +
            "store gamma into 'byuser';\n";
        
        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void nestedTest() throws Throwable {
        String macro = "define group_and_count (A) returns B {\n" +
            "    C = group $A by $0;\n" +
            "    $B = FOREACH C { \n" +
            "       FA = FILTER $A BY user == 'www.xyz.org';\n" +
            "       PA = FA.age;\n" +
            "       DA = DISTINCT PA;\n" +
            "       GENERATE group, COUNT(DA);\n" +
            "   }\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = load 'users' as (user, age, zip);\n" +
            "macro_group_and_count_C_0 = group alpha by ($0);\n" +
            "gamma = FOREACH macro_group_and_count_C_0 { FA = FILTER alpha BY user == 'www.xyz.org'; PA = FA.(age); DA = DISTINCT PA;  GENERATE group, COUNT(DA); } ;\n\n" +
            "store gamma into 'byuser';\n";
        
        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void bincondTest() throws Throwable {
        String macro = "define group_and_count (A) returns B {\n" +
            "    X = FOREACH $A GENERATE f1, f2, f1%f2;\n" +
            "    Y = FOREACH $A GENERATE f2, (f2==1?1:COUNT(B));\n" +
            "    Z = FILTER $A BY f1 is not null;\n" +
            "    $B = FILTER $A BY (f1 matches '.*apache.*');\n" +
            "};\n";
        
        String script = 
            "alpha = LOAD 'data' AS (f1:chararray, f2:int, B:bag{T:tuple(t1:int,t2:int)});\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nalpha = LOAD 'data' AS (f1:chararray, f2:int, B:bag{T:tuple(t1:int,t2:int)});\n" +
            "macro_group_and_count_X_0 = FOREACH alpha GENERATE f1, f2, f1 % f2;\n" +
            "macro_group_and_count_Y_0 = FOREACH alpha GENERATE f2,  (f2 == 1 ? 1 : COUNT(B)) ;\n" +
            "macro_group_and_count_Z_0 = FILTER alpha BY (f1 IS not null);\n" +
            "gamma = FILTER alpha BY (f1 matches '.*apache.*');\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test(expected = RuntimeException.class)  
    public void duplicationTest() throws Throwable {
        String macro = "define group_and_count (A,group_key, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 23);\n" +
            "delta = group_and_count (alpha, age, 32);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        StringReader rd = new StringReader(macro + macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
    }
    
    @Test
    public void simpleImportTest() throws Throwable {
        String macro = "define group_and_count (A,group_key, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        File f = new File("mytest.pig");
        f.deleteOnExit();
        
        FileWriter fw = new FileWriter(f);
        fw.append(macro);
        fw.close();
        
        String script =
            "import 'mytest.pig';\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 23);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\n\nalpha = load 'users' as (user, age, zip);\n" +
            "gamma = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void importUsingSearchPathTest() throws Throwable {
        String macro = "define group_and_count (A,group_key, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        File f = new File("/tmp/mytest2.pig");
        f.deleteOnExit();
        
        FileWriter fw = new FileWriter(f);
        fw.append(macro);
        fw.close();
        
        String script =
            "import 'mytest2.pig';\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 23);\n" +
            "store gamma into 'byuser';\n";
        
        File f1 = new File("myscript.pig");
        f1.deleteOnExit();
        
        FileWriter fw1 = new FileWriter(f1);
        fw1.append(script);
        fw1.close();
        
        String[] args = { "-Dpig.import.search.path=/tmp", "-x", "local", "-c", "myscript.pig" };
        PigStats stats = PigRunner.run(args, null);
 
        assertTrue(stats.isSuccessful());
    }
    
    @Test
    public void negtiveUsingSearchPathTest() throws Throwable {
        String macro = "define group_and_count (A,group_key, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        File f = new File("/tmp/mytest2.pig");
        f.deleteOnExit();
        
        FileWriter fw = new FileWriter(f);
        fw.append(macro);
        fw.close();
        
        String script =
            "import 'mytest2.pig';\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 23);\n" +
            "store gamma into 'byuser';\n";
        
        File f1 = new File("myscript.pig");
        f1.deleteOnExit();
        
        FileWriter fw1 = new FileWriter(f1);
        fw1.append(script);
        fw1.close();
        
        String[] args = { "-x", "local", "-c", "myscript.pig" };
        PigStats stats = PigRunner.run(args, null);
 
        assertTrue(!stats.isSuccessful());
    }
    
    @Test
    public void importTwoMacrosTest() throws Throwable {
        String macro = "define group_and_count (A, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n" +
            "define distinct_with_reducer(A, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        File f = new File("mytest.pig");
        f.deleteOnExit();
        
        FileWriter fw = new FileWriter(f);
        fw.append(macro);
        fw.close();
        
        String script =
            "import 'mytest.pig';\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, 23);\n" +
            "beta = distinct_with_reducer(alpha, 32);\n" +
            "store beta into 'byage';\n" +
            "store gamma into 'byuser';\n";

        StringReader rd = new StringReader(script);
        String s = ParserUtil.expandMacros(rd);

        validate(s);

        String expected =
            "\n\n\nalpha = load 'users' as (user, age, zip);\n" +
            "gamma = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n\n" +
            "beta = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 32;\n\n" +
            "store beta into 'byage';\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void importTwoFilesTest() throws Throwable {
        String macro1 = "define group_and_count (A, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        String macro2 = "define distinct_with_reducer(A, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        File f1 = new File("mytest1.pig");
        f1.deleteOnExit();
        
        FileWriter fw1 = new FileWriter(f1);
        fw1.append(macro1);
        fw1.close();
        
        File f2 = new File("mytest2.pig");
        f2.deleteOnExit();
        
        FileWriter fw2 = new FileWriter(f2);
        fw2.append(macro2);
        fw2.close();
        
        String script =
            "import 'mytest1.pig';\n" +
            "import 'mytest2.pig';\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, 23);\n" +
            "beta = distinct_with_reducer(alpha, 32);\n" +
            "store beta into 'byage';\n" +
            "store gamma into 'byuser';\n";

        StringReader rd = new StringReader(script);
        String s = ParserUtil.expandMacros(rd);

        validate(s);

        String expected =
            "\n\n\n\nalpha = load 'users' as (user, age, zip);\n" +
            "gamma = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n\n" +
            "beta = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 32;\n\n" +
            "store beta into 'byage';\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void noParamTest() throws Throwable {
        String macro = "define group_and_count() returns B {\n" +
            "    D = load 'myfile.txt' using PigStorage('\t') AS (a0:int, a1:int, a2:int);\n" +   
            "    $B = FILTER D BY ($1 == 8) OR (NOT ($0+$2 > $1));\n" +
            "};\n";
        
        String script = 
            "delta = group_and_count();\n" +
            "store delta into 'byage';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nmacro_group_and_count_D_0 = load 'myfile.txt' USING PigStorage('\t') AS (a0:int, a1:int, a2:int);\n" +
            "delta = FILTER macro_group_and_count_D_0 BY (($1 == 8) OR ( NOT ($0 + $2 > $1)));\n\n" +
            "store delta into 'byage';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void noReturnTest() throws Throwable {
        String macro = "define group_and_count() returns void {\n" +
            "    D = load 'myfile.txt' using PigStorage() AS (a0:int, a1:int, a2:int);\n" +   
            "    store D into 'myoutput';\n" +
            "};\n";
        
        String script = 
            "dummy = group_and_count();\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
        
        String expected =
            "\nmacro_group_and_count_D_0 = load 'myfile.txt' USING PigStorage() AS (a0:int, a1:int, a2:int);\n" +
            "store macro_group_and_count_D_0 INTO 'myoutput';\n\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void noReturnTest2() throws Throwable {
        String macro = "define group_and_count(input, output) returns void {\n" +
            "    D = load '$input';\n" +   
            "    store D into '$output';\n" +
            "};\n";
        
        String script = 
            "dummy = group_and_count('myfile.txt', '/tmp/myoutput');\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
       
        String expected =
            "\nmacro_group_and_count_D_0 = load 'myfile.txt';\n" +
            "store macro_group_and_count_D_0 INTO '/tmp/myoutput';\n\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test(expected = RuntimeException.class)  
    public void negativeTest() throws Throwable {
        String macro = "define group_and_count (A,group_key, size) returns B {\n" +
            "    $B = limit $A $size;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, 20);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
    }   
    
    @Test(expected = RuntimeException.class)  
    public void negativeTest2() throws Throwable {
        String macro = "define group_and_count (A,group_key, size) returns B {\n" +
            "    $B = limit $A $size;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count ();\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
    }   
    
    @Test(expected = RuntimeException.class)
    public void negativeTest3() throws Throwable {
        String macro = "define group_and_count (A,C) returns B, D {\n" +
            "    $B = JOIN $A BY user, $C BY user;\n" +
            "    $D = JOIN $A BY $0, $C BY $1 using 'skewed' parallel 5;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        StringReader rd = new StringReader(macro + script);
        String s = ParserUtil.expandMacros(rd);
        
        validate(s);
    }
    
    @Test
    public void recursiveMacrosTest() throws Throwable {
        String macro1 = "define group_and_partition (A, group_key, reducers) returns B {\n" +
            "    C = group $A by $group_key partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "    $B = foreach_count(C, $A);" +
            "};\n";
        
        String macro2 = "define foreach_count(A, C) returns B {\n" +
            "    $B = foreach $A generate group, COUNT($C);\n" +
            "};\n";
        
        
        String script = macro2 + macro1 +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_partition (alpha, user, 23);\n" +
            "store gamma into 'byuser';\n";

        StringReader rd = new StringReader(script);
        String s = ParserUtil.expandMacros(rd);

        validate(s);

        String expected =
            "\n\nalpha = load 'users' as (user, age, zip);\n" +
            "macro_group_and_partition_C_0 = group alpha by (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n" +
            "gamma = foreach macro_group_and_partition_C_0 generate group, COUNT(alpha);\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void recursiveMacrosTest2() throws Throwable {
        String macro1 = "define foreach_count(A, C) returns B {\n" +
        "    $B = foreach $A generate group, COUNT($C);\n" +
        "};\n";
        
        String macro2 = "define group_and_partition (A, group_key, reducers) returns B {\n" +
            "    C = group $A by $group_key partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "    $B = foreach_count(C, $A);\n" +
            "};\n";
        
        String macro3 = "define load_and_group() returns B {\n" +
            "   alpha = load 'users' as (user, age, zip);\n" +
            "   $B = group_and_partition(alpha, user, 30);\n" +
            "};\n";
        
        String script = macro1 + macro2 + macro3 +
            "gamma = load_and_group ();\n" +
            "store gamma into 'byuser';\n";

        StringReader rd = new StringReader(script);
        String s = ParserUtil.expandMacros(rd);

        validate(s);

        String expected =
            "\n\n\nmacro_load_and_group_alpha_0 = load 'users' as (user, age, zip);\n" +
            "macro_load_and_group_C_0 = group macro_load_and_group_alpha_0 by (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 30;\n" +
            "gamma = foreach macro_load_and_group_C_0 generate group, COUNT(macro_load_and_group_alpha_0);\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void sequenceMacrosTest() throws Throwable {
        String macro1 = "define foreach_count(A, C) returns B {\n" +
        "    $B = foreach $A generate group, COUNT($C);\n" +
        "};\n";
        
        String macro2 = "define group_and_partition (A, group_key, reducers) returns B {\n" +
            "    $B = group $A by $group_key partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        String script = macro1 + macro2 +
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = group_and_partition (alpha, user, 20);\n" +
            "gamma = foreach_count(beta, alpha);\n" +
            "store gamma into 'byuser';\n";

        StringReader rd = new StringReader(script);
        String s = ParserUtil.expandMacros(rd);

        validate(s);

        String expected =
            "\n\nalpha = load 'users' as (user, age, zip);\n" +
            "beta = group alpha by (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 20;\n\n" +
            "gamma = foreach beta generate group, COUNT(alpha);\n\n" +
            "store gamma into 'byuser';\n";

        Assert.assertEquals(expected, s);
    }
    
    @Test(expected = RuntimeException.class)
    public void selfRecursiveTest() throws Throwable {
        String macro1 = "define group_and_partition (A, group_key, reducers) returns B {\n" +
            "    C = group $A by $group_key partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "    $B = group_and_partition(C, age, 34);" +
            "};\n";
        
        String script = macro1 +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_partition (alpha, user, 23);\n" +
            "store gamma into 'byuser';\n";

        StringReader rd = new StringReader(script);
        String s = ParserUtil.expandMacros(rd);

        validate(s);
    }
    
    @Test(expected = RuntimeException.class)
    public void cyclicRecursiveTest() throws Throwable {
        String macro1 = "define group_and_partition (A, group_key, reducers) returns B {\n" +
            "    C = group $A by $group_key partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "    $B = foreach_count(C, $A);" +
            "};\n";
        
        String macro2 = "define foreach_count(A, C) returns B {\n" +
            "    $B = foreach $A generate group, COUNT($C);\n" +
            "    D = group_and_partition($C, age, 23);" +
            "};\n";
        
        
        String script = macro2 + macro1 +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_partition (alpha, user, 23);\n" +
            "store gamma into 'byuser';\n";

        StringReader rd = new StringReader(script);
        String s = ParserUtil.expandMacros(rd);

        validate(s);
    }
    
    @Test
    public void typecastTest() throws Throwable {
        String macro = 
            "a = load '1.txt' as (a0);" +
            "b = foreach a generate flatten( (bag{tuple(map[])})a0 ) as b0:map[];" +
            "c = foreach b generate (long)b0#'key1';";
        
        testMacro(macro);
    }
    
    @Test
    public void test17() throws Throwable {
        String macro = 
            "a = load '/user/pig/tests/data/singlefile/studentnulltab10k' as (name:chararray, age:int, gpa:double);" +
            "b = foreach a generate (int)((int)gpa/((int)gpa - 1)) as norm_gpa:int;" + 
            "c = foreach b generate (norm_gpa is not null? norm_gpa: 0);" +
            "store c into '/user/pig/out/jianyong.1297229709/Types_37.out';";
            
        testMacro(macro);
    }
    
    @Test
    public void test18() throws Throwable {
        String macro = "define mymacro() returns dummy {" +
            "a = load '/user/pig/tests/data/singlefile/studenttab10k';" +
            "b = group a by $0;" +
            "c = foreach b {c1 = order $1 by * using org.apache.pig.test.udf.orderby.OrdDesc; generate flatten(c1); };" +
            "store c into '/user/pig/out/jianyong.1297305352/Order_15.out';};";
            
        String script = macro +
            "dummy = mymacro();\n";
        

        StringReader rd = new StringReader(script);
        String s = ParserUtil.expandMacros(rd);

        //validate(s);
        String expected =
            "macro_mymacro_a_0 = load '/user/pig/tests/data/singlefile/studenttab10k';\n" +
            "macro_mymacro_b_0 = group macro_mymacro_a_0 by ($0);\n" +
            "macro_mymacro_c_0 = foreach macro_mymacro_b_0 { c1 = order $1 BY * USING org.apache.pig.test.udf.orderby.OrdDesc;  generate flatten(c1) ; } ;\n" +
            "store macro_mymacro_c_0 INTO '/user/pig/out/jianyong.1297305352/Order_15.out';\n\n";
        
        Assert.assertEquals(expected, s);
    }
    
    @Test
    public void test19() throws Throwable {
        String macro = 
            "a = load '/user/pig/tests/data/singlefile/studenttab10k';" +
            "b = group a by $0;" +
            "c = foreach b {c1 = order $1 by $1; generate flatten(c1), MAX($1.$1); };" +
            "store c into '/user/pig/out/jianyong.1297305352/Order_17.out';";
            
        testMacro(macro);
    }
    
    @Test
    public void test20() throws Throwable {
        String macro = 
            "a = load 'x' as (u,v);" +
            "b = load 'y' as (u,w);" +
            "c = join a by u, b by u;" +
            "d = foreach c generate a::u, b::u, w;";
            
        testMacro(macro);
    }
    
    @Test
    public void test21() throws Throwable {
        String macro = 
        "a = load '1.txt' as ( u, v, w : int );" +
        "b = foreach a generate * as ( x, y, z ), flatten( u ) as ( r, s ), flatten( v ) as d, w + 5 as e:int;";
            
        testMacro(macro);
    }
    
    @Test
    public void test22() throws Throwable {
        String macro =
            "a = load '1.txt' as ( u : bag{}, v : bag{tuple(x, y)} );" +
            "b = load '2.x' as ( t : {}, u : {(r,s)}, v : bag{ T : tuple( x, y ) }, w : bag{(z1, z2)} );" +
            "c = load '3.x' as p : int;";
            
        testMacro(macro);
    }

    @Test
    public void test23() throws Throwable {
        String macro = 
        "a = load '1.txt' as ( u, v, w : int );" +
        "b = foreach a generate * as ( x, y, z ), flatten( u ) as ( r, s ), flatten( v ) as d, w + 5 as e:int;";
            
        testMacro(macro);
    }
    
    @Test
    public void test24() throws Throwable {
        String macro = 
        "A = load 'x' as ( u:bag{tuple(x, y)}, v:long, w:bytearray); " + 
        "B = foreach A generate u.(x, y), v, w; " +
        "C = store B into 'output';";
            
        testMacro(macro);
    }
    
    @Test
    public void test25() throws Throwable {
        String macro = 
        "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
        "B = foreach A { R = a; P = c * 2; Q = P + d; S = R.u; T = limit S 100; generate Q, R, S, T, c + d/5; };" +
        "store B into 'y';";
            
        testMacro(macro);
    }
    
    @Test
    public void test26() throws Throwable {
        String content =
        "A = load 'x' as ( u:bag{tuple(x, y)}, v:long, w:bytearray); " + 
        "B = foreach A generate u.(x, $1), $1, w; " +
        "C = store B into 'output';";
            
        testMacro(content);
    }
    
    @Test
    public void test27() throws Throwable {
        String content =
            "A = load 'x' as ( u:bag{} ); " + 
            "B = foreach A generate u.$100; " +
            "C = store B into 'output';";
            
        testMacro(content);
    }
    
    @Test
    public void test28() throws Throwable {
        String content =
            "A = load 'x'; " + 
            "B = foreach A generate $1, $1000; " +
            "C = store B into 'output';";
            
        testMacro(content);
    }
    
    @Test
    public void test29() throws Throwable {
        String content =
            "A = load 'x'; " + 
            "B = load 'y' as ( u : int, v : chararray );" +
            "C = foreach A generate B.$1, $0; " +
            "D = store C into 'output';";
            
        testMacro(content);
    }
    
    @Test
    public void test30() throws Throwable {
        String content =
            "A = load 'x'; " + 
            "B = load 'y' as ( u : int, v : chararray );" +
            "C = foreach A generate B.$1, $0; " +
            "D = store C into 'output';";
            
        testMacro(content);
    }
    
    @Test
    public void test31() throws Throwable {
        String content =
            "A = load 'x'; " + 
            "B = load 'y' as ( u : int, v : chararray );" +
            "C = foreach A generate B.v, $0; " +
            "D = store C into 'output';";
            
        testMacro(content);
    }
    
    public void test1() throws Throwable {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = limit A 100; " +
                       "C = filter B by 2 > 1; " +
                       "D = load 'y' as (d1, d2); " +
                       "E = join C by ( $0, $1 ), D by ( d1, d2 ) using 'replicated' parallel 16; " +
                       "F = store E into 'output';";
        testMacro( query );

    }

    //@Test
    public void test2() throws Throwable {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = distinct A partition by org.apache.pig.Identity; " +
                       "C = sample B 0.49; " +
                       "D = order C by $0, $1; " +
                       "E = load 'y' as (d1, d2); " +
                       "F = union onschema D, E; " +
                       "G = load 'z' as (g1:int, g2:tuple(g21, g22)); " +
                       "H = cross F, G; " +
                       "split H into I if 10 > 5, J if 'world' eq 'hello', K if 77 <= 200; " +
                       "L = store J into 'output';";
        testMacro( query );
    }

    @Test
    public void test3() throws Throwable {
        String query = "a = load '1.txt'  as (name, age, gpa);" + 
                       "b = group a by name PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner2;" +
                       "c = foreach b generate group, COUNT(a.age);" +
                       "store c into 'y';";
        testMacro( query );
    }
    

    @Test
    public void test4() throws Throwable {
        String query = "A = load 'x'; " + 
                       "B = mapreduce '" + "myjar.jar" + "' " +
                           "Store A into 'table_testNativeMRJobSimple_input' "+
                           "Load 'table_testNativeMRJobSimple_output' "+
                           "`org.apache.pig.test.utils.WordCount -files " + "file " +
                           "table_testNativeMRJobSimple_input table_testNativeMRJobSimple_output " +
                           "stopworld.file" + "`;" +
                        "C = Store B into 'output';";
        testMacro( query );
    }

    // Test define function.
    @Test
    public void test5() throws Throwable {
        String query = "define myudf org.apache.pig.builtin.PigStorage( ',' );" +
                       "A = load 'x' using myudf;" +
                       "store A into 'y';";
        testMacro( query );
    }

    @Test
    public void test6() throws Throwable {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = group A by ( a, $2 );" +
                       "store B into 'y';";
        testMacro( query );
    }

    @Test
    public void test7() throws Throwable {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = foreach A generate a, $2;" +
                       "store B into 'y';";
        testMacro( query );
    }

    @Test
    public void test8() throws Throwable {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = group A by a;" +
                       "C = foreach B { S = A.b; generate S; };" +
                       "store C into 'y';";
        testMacro( query );
    }
    
    @Test
    public void test9() throws Throwable {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { R = a; S = R.u; T = limit S 100; generate S, T, c + d/5; };" +
                       "store B into 'y';";
        testMacro( query );
    }

    @Test
    public void test10() throws Throwable {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { S = a; T = limit S 100; generate T; };" +
                       "store B into 'y';";
        testMacro( query );
    }

    @Test
    public void test11() throws Throwable {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { T = limit a 100; generate T; };" +
                       "store B into 'y';";
        testMacro( query );
    }

    //@Test
    public void test12() throws Throwable {
        String query = "define CMD `perl GroupBy.pl '\t' 0 1` ship('/homes/jianyong/pig_harness/libexec/PigTest/GroupBy.pl');" +
                       "A = load '/user/pig/tests/data/singlefile/studenttab10k';" +
                       "B = group A by $0;" +
                       "C = foreach B {" +
                       "   D = order A by $1; " +
                       "   generate flatten(D);" +
                       "};" +
                       "E = stream C through CMD;" +
                       "store E into '/user/pig/out/jianyong.1297238871/ComputeSpec_8.out';";
        testMacro( query );
    }
    
    //@Test
    public void test13() throws Throwable {
        String query = "define CMD `perl PigStreaming.pl` ship('/homes/jianyong/pig_harness/libexec/PigTest/PigStreaming.pl') stderr('CMD');" +
                       "A = load '/user/pig/tests/data/singlefile/studenttab10k';" +
                       "C = stream A through CMD;" +
                       "store C into '/user/pig/out/jianyong.1297238871/StreamingPerformance_1.out';";
        testMacro( query );
    }
    
    @Test
    public void test14() throws Throwable {
        String query = "a = load '/user/pig/tests/data/singlefile/studenttab10k' using PigStorage() as (name, age:int, gpa);" +
                       "b = load '/user/pig/tests/data/singlefile/votertab10k' as (name, age, registration, contributions);" +
                       "e = cogroup a by name, b by name parallel 8;" +
                       "f = foreach e generate group,  SUM(a.age) as s;" +
                       "g = filter f by s>0;" +
                       "store g into '/user/pig/out/jianyong.1297323675/Accumulator_1.out';";
        testMacro( query );
    }
    
    @Test
    public void test15() throws Throwable {
        String query = "a = load '/user/pig/tests/data/singlefile/studenttab10k' using PigStorage() as (name, age, gpa);" +
                       "b = group a all;" +
                       "c = foreach b generate AVG(a.age) as avg; " +
                       "d = load '/user/pig/tests/data/singlefile/votertab10k' using PigStorage() as (name, age, registration, contributions);" +
                       "e = group d all;" +
                       "f = foreach e generate AVG(d.age) as avg;" +
                       "y = foreach a generate age/c.avg, age/f.avg;" +
                       "store y into '/user/pig/out/jianyong.1297323675/Scalar_4.out';";
        testMacro( query );
    }
    
    @Test
    public void test16() throws Throwable {
        String query = "AA = load '/user/pig/tests/data/singlefile/studenttab10k';" +
                       "A = foreach (group (filter AA by $0 > 0) all) generate flatten($1);" +
                       "store A into '/user/pig/out/jianyong.1297323675/Scalar_4.out';";
        testMacro( query );
    }

    @Test
    public void testFilter() throws Throwable {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = filter A by 2 > 1; ";
        testMacro( query );
    }

    @Test
    public void testScopedAlias() throws Throwable {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray);" + 
                       "B = load 'y' as ( u:int, x:int, y:chararray);" +
                       "C = join A by u, B by u;" +
                       "D = foreach C generate A::u, B::u, v, x;" +
                       "store D into 'z';";
        testMacro ( query );
    }

    
    private void testMacro(String content) throws Throwable {
        String macro = "define mymacro() returns dummy {" +
            content + "};";
            
        String script = macro +
            "dummy = mymacro();\n";
        
        StringReader rd = new StringReader(script);
        String s = ParserUtil.expandMacros(rd);

        validate(s);
    }
    
    private void validate(String s) throws Throwable {
        PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
        BufferedReader br = new BufferedReader(new StringReader(s));
        Grunt grunt = new Grunt(br, pigContext);
        
        File f = new File("macro_expansion.txt");
        FileWriter w = new FileWriter(f);
        w.append(s);
        w.close();
        
        grunt.checkScript("macro_expansion.txt");
        f.delete();
    }

}
