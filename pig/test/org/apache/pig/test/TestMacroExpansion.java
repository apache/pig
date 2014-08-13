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
import static org.junit.Assert.assertFalse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigRunner;
import org.apache.pig.impl.PigContext;
import org.apache.pig.parser.DryRunGruntParser;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMacroExpansion {
    
    private static String groupAndCountMacro = "define group_and_count (A,group_key, reducers) returns B {\n" +
            "    $B = distinct $A partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
    private static String garbageMacroContent = "&?:#garbage-!";
    static File command;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // Perl script 
        String[] script = 
            new String[] {
                          "#!/usr/bin/perl",
                          "open(INFILE,  $ARGV[0]) or die \"Can't open \".$ARGV[0].\"!: $!\";",
                          "while (<INFILE>) {",
                          "  chomp $_;",
                          "  print STDOUT \"$_\n\";",
                          "  print STDERR \"STDERR: $_\n\";",
                          "}",
                         };
        command = Util.createInputFile("script", "pl", script);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test 
    public void firstTest() throws Exception {
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
                
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "macro_group_and_count_D_0 = group alpha by (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 50;\n" +
            "gamma = foreach macro_group_and_count_D_0 generate group, COUNT(alpha);\n" +
            "macro_group_and_count_D_1 = group alpha by (age) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 50;\n" +
            "delta = foreach macro_group_and_count_D_1 generate group, COUNT(alpha);\n" +
            "store gamma INTO 'byuser';\n" +
            "store delta INTO 'byage';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void distinctTest() throws Exception {
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 23);\n" +
            "delta = group_and_count (alpha, age, 32);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n" +
            "delta = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 32;\n" +
            "store gamma INTO 'byuser';\n" +
            "store delta INTO 'byage';\n";
        
        verify(groupAndCountMacro + script, expected);
    }   
    
    @Test
    public void limitTest() throws Exception {
        String macro = "define group_and_count (A,group_key, size) returns B {\n" +
            "    $B = limit $A $size;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 20);\n" +
            "delta = group_and_count (alpha, age, 39);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = limit alpha 20;\n" +
            "delta = limit alpha 39;\n" +
            "store gamma INTO 'byuser';\n" +
            "store delta INTO 'byage';\n";
        
        verify(macro + script, expected);
    }   
    
    @Test
    public void sampleTest() throws Exception {
        String macro = "define group_and_count (A, rate) returns B {\n" +
            "    $B = sample $A $rate;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, 0.01);\n" +
            "delta = group_and_count (alpha, 0.002);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = sample alpha 0.01;\n" +
            "delta = sample alpha 0.002;\n" +
            "store gamma INTO 'byuser';\n" +
            "store delta INTO 'byage';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void orderbyTest() throws Exception {
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
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = ORDER alpha BY user ASC, age DESC PARALLEL 3;\n" +
            "macro_group_and_count_C_0 = ORDER alpha BY * ASC PARALLEL 3;\n" +
            "delta = ORDER alpha BY age ASC, zip DESC PARALLEL 3;\n" +
            "macro_group_and_count_C_1 = ORDER alpha BY * ASC PARALLEL 3;\n" +
            "store gamma INTO 'byuser';\n" +
            "store delta INTO 'byage';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void crossTest() throws Exception {
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
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "gamma = CROSS alpha, beta partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "delta = CROSS beta, alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "store gamma INTO 'byuser';\n" +
            "store delta INTO 'byage';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void loadTest() throws Exception {
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
        
        String expected = 
            "macro_group_and_count_D_0 = load 'myfile.txt' USING PigStorage('\t') AS (name:chararray, age:int, gpa:float);\n" +
            "gamma = load 'myfile' USING PigStorage('\t') AS (F:(f1:int, f2:int, f3:int), T:(t1:chararray, t2:int));\n" +
            "macro_group_and_count_E_0 = load 'myfile.txt' USING org.apache.pig.builtin.PigStorage('\t') AS B:bag{T:(t1:int, t2:int, t3:int)};\n" +
            "macro_group_and_count_F_0 = load 'myfile.txt' USING org.apache.pig.builtin.PigStorage('\t') AS (T1:(f1:int, f2:int), B:bag{T:(t1:float, t2:float)}, M:map[]);\n" +
            "macro_group_and_count_D_1 = load 'myfile.txt' USING PigStorage('\t') AS (name:chararray, age:int, gpa:float);\n" +
            "delta = load 'mydir' USING PigStorage('\t') AS (F:(f1:int, f2:int, f3:int), T:(t1:chararray, t2:int));\n" +
            "macro_group_and_count_E_1 = load 'myfile.txt' USING org.apache.pig.builtin.PigStorage('\t') AS B:bag{T:(t1:int, t2:int, t3:int)};\n" +
            "macro_group_and_count_F_1 = load 'myfile.txt' USING org.apache.pig.builtin.PigStorage('\t') AS (T1:(f1:int, f2:int), B:bag{T:(t1:float, t2:float)}, M:map[]);\n" +
            "store gamma INTO 'byuser';\n" +
            "store delta INTO 'byage';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void storeTest() throws Exception {
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
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "gamma = CROSS alpha, beta partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "STORE alpha INTO 'myoutput' USING PigStorage('*');\n" +
            "delta = CROSS beta, alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "STORE beta INTO 'myoutput' USING PigStorage('*');\n" +
            "store gamma INTO 'byuser';\n" +
            "store delta INTO 'byage';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void streamTest() throws Exception {
        String macro = "define group_and_count (A) returns B {\n" +
            "    $B = STREAM $A THROUGH `stream.pl -n 5`;\n" +
            "    DEFINE mycmd `stream.pl -n 5`;\n" +
            "    $B = STREAM $A THROUGH mycmd;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = STREAM alpha THROUGH `stream.pl -n 5`;\n" + 
            "DEFINE mycmd `stream.pl -n 5`;\n" + 
            "gamma = STREAM alpha THROUGH mycmd;\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test 
    public void defineTest() throws Exception {
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
        
        String expected = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "DEFINE CMD `perl PigStreaming.pl - nameMap` input(stdin USING PigStreaming(',')) output (stdout USING PigStreaming(','));\n" +
            "DEFINE mycmd `stream_cmd input file.dat`;\n" +
            "DEFINE Z `stream.pl` stderr ('<dir>' LIMIT 100);\n" +
            "gamma = STREAM alpha THROUGH CMD;\n" +
            "macro_group_and_count_D_0 = STREAM alpha THROUGH mycmd;\n" +
            "macro_group_and_count_F_0 = STREAM alpha THROUGH Z;\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test 
    public void defineTest2() throws Exception {
        String macro = "define group_and_count (A) returns B {\n" +
            "    DEFINE CMD `perl stream.pl data.gz` SHIP('"+Util.encodeEscape(command.toString())+"') CACHE('"+Util.encodeEscape(command.toString())+"');\n" +
            "    $B = STREAM $A THROUGH CMD;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        verify(macro + script);
    }
    
    @Test
    public void groupTest() throws Exception {
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
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "macro_group_and_count_D_0 = group alpha by (user) parallel 50;\n" +
            "gamma = foreach macro_group_and_count_D_0 generate group, COUNT(alpha);\n" +
            "macro_group_and_count_X_0 = GROUP alpha BY (user) USING 'collected';\n" +
            "macro_group_and_count_Y_0 = GROUP alpha BY (user, age) USING 'merge';\n" +
            "macro_group_and_count_Z_0 = GROUP alpha ALL;\n" + 
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void cogroupTest() throws Exception {
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
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "macro_group_and_count_D_0 = cogroup alpha by (user), beta by (user) parallel 50;\n" +
            "gamma = cogroup alpha by (user) inner, beta by (user) inner parallel 50;\n" +
            "macro_group_and_count_Y_0 = COGROUP alpha BY (user, age), beta by (user, view) USING 'merge';\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void unionTest() throws Exception {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    D = union $A, $C;\n" +
            "    $B = union onschema $A, $C;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "gamma = group_and_count (alpha, beta);\n" +
            "store gamma into 'byuser';\n";
        
        String expected = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, page, view);\n" +
            "macro_group_and_count_D_0 = union alpha, beta;\n" +
            "gamma = union onschema alpha, beta;\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void splitTest() throws Exception {
        String macro = "define group_and_count (A,key) returns B {\n" +
            "    SPLIT $A INTO $B IF $key<7, Y IF $key==5, Z IF ($key<6 OR $key>6);\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, age);\n" +
            "store gamma into 'byuser';\n";
        
        String expected = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "SPLIT alpha INTO gamma IF age < 7, macro_group_and_count_Y_0 IF age == 5, macro_group_and_count_Z_0 IF (age < 6) OR (age > 6);\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }

    @Test
    public void splitOtherwiseTest() throws Exception {
        String macro = "define group_and_count (A,key) returns B {\n" +
            "SPLIT $A INTO $B IF $key<7, Y IF $key==5, Z OTHERWISE;\n" +
            "};\n";

        String script =
            "alpha = load 'users' as (f1:int);\n" +
            "gamma = group_and_count (alpha, f1);\n" +
            "store gamma into 'byuser';\n";

        String expected =
            "alpha = load 'users' as f1:int;\n" +
            "SPLIT alpha INTO gamma IF f1 < 7, macro_group_and_count_Y_0 IF f1 == 5, macro_group_and_count_Z_0 OTHERWISE;\n" +
            "store gamma INTO 'byuser';\n";

        verify(macro + script, expected);
    }
    
    @Test
    public void mapreduceTest() throws Exception {
        String macro = "define group_and_count (A) returns B {\n" +
            "    $B = MAPREDUCE 'wordcount.jar' STORE $A INTO 'inputDir' LOAD 'outputDir' " + 
            "AS (word:chararray, count: int) `org.myorg.WordCount inputDir outputDir`;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = MAPREDUCE 'wordcount.jar' STORE alpha INTO 'inputDir' LOAD 'outputDir' AS (word:chararray, count:int) `org.myorg.WordCount inputDir outputDir`;\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void joinTest() throws Exception {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    $B = JOIN $A BY user, $C BY user using 'replicated' partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "    $B = JOIN $A BY $0, $C BY $1 using 'skewed' parallel 5;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = JOIN alpha BY (user), beta BY (user) USING 'replicated' partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "gamma = JOIN alpha BY ($0), beta BY ($1) USING 'skewed' parallel 5;\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void multiOutputsTest() throws Exception {
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
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = JOIN alpha BY (user), beta BY (user) USING 'replicated' partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "sigma = JOIN alpha BY ($0), beta BY ($1) USING 'skewed' parallel 5;\n" +
            "store gamma INTO 'byuser';\n" +
            "store sigma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void parameterSubstitutionTest() throws Exception {
        String macro = "define group_and_count (A,C) returns B, D {\n" +
            "    $B = JOIN $A BY user, $C BY user using 'replicated' partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "    $D = JOIN $A BY $0, $C BY $1 using 'skewed' parallel 5;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma, sigma = group_and_count (alpha,beta);\n" +
            "store gamma into '$output1';\n" +
            "store sigma into '$output2';\n";
        
        File f1 = new File("myscript.pig");
        f1.deleteOnExit();
        
        FileWriter fw1 = new FileWriter(f1);
        fw1.append(macro).append(script);
        fw1.close();
        
        String[] args = { "-x", "local", "-p", "output1=byuser", "-p", "output2=byage", "-c", "myscript.pig" };
        PigStats stats = PigRunner.run(args, null);
 
        assertTrue(stats.isSuccessful());
    }
    
    @Test 
    public void outerTest() throws Exception {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    $B = JOIN $A BY user right outer, $C BY user partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "    D = JOIN $A BY user LEFT, $C BY user;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        String expected = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = JOIN alpha BY (user) right outer, beta BY (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 5;\n" +
            "macro_group_and_count_D_0 = JOIN alpha BY (user) LEFT, beta BY (user);\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void filterTest() throws Exception {
        String macro = "define group_and_count (A) returns B {\n" +
            "    $B = FILTER $A BY ($1 == 8) OR (NOT ($0+$2 > $1));\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        String expected = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = FILTER alpha BY (($1 == 8) OR ( NOT ($0 + $2 > $1)));\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test 
    public void foreachTest() throws Exception {
        String macro = "define group_and_count (A) returns B {\n" +
            "    $B = foreach $A generate $0, $2;\n" +
            "    C = group $A by $0;\n" +
            "    X = FOREACH C GENERATE group, SUM ($A.$1);\n" +
            "    Y = FOREACH C GENERATE group, FLATTEN($A);\n" +
            "    Z = FOREACH C GENERATE FLATTEN($A.($0, $2)), FLATTEN($A.age);\n" +
            "    X = FOREACH $A GENERATE $1+$2 AS f1:int;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age:int, zip:int);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        String expected =
            "alpha = load 'users' as (user, age:int, zip:int);\n" +
            "gamma = foreach alpha generate $0, $2;\n" +
            "macro_group_and_count_C_0 = group alpha by ($0);\n" +
            "macro_group_and_count_X_0 = FOREACH macro_group_and_count_C_0 GENERATE group, SUM(alpha.($1));\n" +
            "macro_group_and_count_Y_0 = FOREACH macro_group_and_count_C_0 GENERATE group, FLATTEN(alpha) ;\n" +
            "macro_group_and_count_Z_0 = FOREACH macro_group_and_count_C_0 GENERATE FLATTEN(alpha.($0, $2)) , FLATTEN(alpha.(age)) ;\n" +
            "macro_group_and_count_X_0 = FOREACH alpha GENERATE $1 + $2 AS f1:int;\n" +
            "store gamma INTO 'byuser';\n";
            
        verify(macro + script, expected);
    }
    
    @Test
    public void nestedTest() throws Exception {
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
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "macro_group_and_count_C_0 = group alpha by ($0);\n" +
            "gamma = FOREACH macro_group_and_count_C_0 { FA = FILTER alpha BY user == 'www.xyz.org'; PA = FA.(age); DA = DISTINCT PA;  GENERATE group, COUNT(DA); } ;\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }
    
    @Test
    public void bincondTest() throws Exception {
        String macro = "define group_and_count (A) returns B {\n" +
            "    X = FOREACH $A GENERATE f1, f2, f0%f2;\n" +
            "    Y = FOREACH $A GENERATE f2, (f2==1?1:COUNT(B));\n" +
            "    Z = FILTER $A BY f1 is not null;\n" +
            "    $B = FILTER $A BY (f1 matches '.*apache.*');\n" +
            "};\n";
        
        String script = 
            "alpha = LOAD 'data' AS (f0:long, f1:chararray, f2:int, B:bag{T:tuple(t1:int,t2:int)});\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        String expected = 
            "alpha = LOAD 'data' AS (f0:long, f1:chararray, f2:int, B:bag{T:(t1:int, t2:int)});\n" +
            "macro_group_and_count_X_0 = FOREACH alpha GENERATE f1, f2, f0 % f2;\n" +
            "macro_group_and_count_Y_0 = FOREACH alpha GENERATE f2,  (f2 == 1 ? 1 : COUNT(B)) ;\n" +
            "macro_group_and_count_Z_0 = FILTER alpha BY (f1 IS not null);\n" +
            "gamma = FILTER alpha BY (f1 matches '.*apache.*');\n" +
            "store gamma INTO 'byuser';\n";
            
        verify(macro + script, expected);
    }
    
    @Test
    public void dupicatedMacroNameTest() throws Throwable {
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 23);\n" +
            "delta = group_and_count (alpha, age, 32);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
        
        String expectedErr = "Reason: Duplicated macro name 'group_and_count'";
        
        validateFailure(groupAndCountMacro + groupAndCountMacro + script, expectedErr);
    }
    
    @Test
    public void simpleImportTest() throws Exception {
        createFile("mytest.pig", groupAndCountMacro);
        
        String script =
            "import 'mytest.pig';\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 23);\n" +
            "store gamma into 'byuser';\n";
        
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n" +
            "store gamma INTO 'byuser';\n";
            
        verify(script, expected);
    }
    
    @Test 
    public void importUsingSearchPathTest() throws Exception {
        createFile("/tmp/mytest2.pig", garbageMacroContent);
        assertTrue(verifyImportUsingSearchPath("/tmp/mytest2.pig", "mytest2.pig", "/tmp"));
    }

    @Test
    public void importUsingSearchPathTest2() throws Exception {
        createFile("/tmp/mytest2.pig", garbageMacroContent);
        assertTrue(verifyImportUsingSearchPath("mytest2.pig", "./mytest2.pig", "/tmp"));
    }

    @Test
    public void importUsingSearchPathTest3() throws Exception {
        createFile("/tmp/mytest2.pig", garbageMacroContent);
        assertTrue(verifyImportUsingSearchPath("../mytest2.pig", "../mytest2.pig", "/tmp"));
    }

    @Test
    public void importUsingSearchPathTest4() throws Exception {
        createFile("/tmp/pigtestdir/tmp/mytest2.pig", garbageMacroContent);
        assertTrue(verifyImportUsingSearchPath("/tmp/mytest2.pig", "/tmp/mytest2.pig", "/tmp/pigtestdir/"));
    }

    @Test
    public void importUsingSearchPathTest5() throws Exception {
        createFile("/tmp/mytest2a.pig", garbageMacroContent);
        createFile("mytest2a.pig", groupAndCountMacro);
        assertTrue(verifyImportUsingSearchPath("/tmp/mytest2a.pig", "mytest2a.pig", "/tmp", false));
    }
    
    @Test
    public void negativeUsingSearchPathTest() throws Exception {
        /* Delete the mytest3.pig file in negativeUsingSearchPathTest, just in
         * case negativeUsingSearchPathTest2 is executed first and a garbage mytest3.pig
         * file is created.
         */
        new File("mytest3.pig").delete();
        assertFalse(verifyImportUsingSearchPath("/tmp/mytest3.pig", "mytest3.pig", null));
    }

    @Test
    public void negativeUsingSearchPathTest2() throws Exception {
        createFile("mytest3.pig", garbageMacroContent);
        assertFalse(verifyImportUsingSearchPath("/tmp/mytest3.pig", "mytest3.pig", "/tmp"));
    }

    @Test
    public void negativeUsingSearchPathTest3() throws Exception {
        createFile("/tmp/mytest3a.pig", garbageMacroContent);
        assertFalse(verifyImportUsingSearchPath("/tmp/mytest3a.pig", "mytest3a.pig", "/tmp", false));
    }
    
    @Test
    public void importTwoMacrosTest() throws Exception {
        String macro = "define group_and_count (A, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n" +
            "define distinct_with_reducer(A, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        createFile("mytest.pig", macro);
        
        String script =
            "import 'mytest.pig';\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, 23);\n" +
            "beta = distinct_with_reducer(alpha, 32);\n" +
            "store beta into 'byage';\n" +
            "store gamma into 'byuser';\n";

        String expected = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n" +
            "beta = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 32;\n" +
            "store beta INTO 'byage';\n" +
            "store gamma INTO 'byuser';\n";
            
        verify(script, expected);
    }
    
    @Test
    public void importTwoFilesTest() throws Exception {
        String macro1 = "define group_and_count (A, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        String macro2 = "define distinct_with_reducer(A, reducers) returns B {\n" +
            "    $B = distinct $A partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "};\n";
        
        createFile("mytest4.pig", macro1);
        createFile("mytest5.pig", macro2);
        
        String script =
            "import 'mytest4.pig';\n" +
            "import 'mytest5.pig';\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, 23);\n" +
            "beta = distinct_with_reducer(alpha, 32);\n" +
            "store beta into 'byage';\n" +
            "store gamma into 'byuser';\n";

        String expected = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n" +
            "beta = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 32;\n" +
            "store beta INTO 'byage';\n" +
            "store gamma INTO 'byuser';\n";
            
        verify(script, expected);
    }
    
    @Test
    public void noParamTest() throws Exception {
        String macro = "define group_and_count() returns B {\n" +
            "    D = load 'myfile.txt' using PigStorage('\t') AS (a0:int, a1:int, a2:int);\n" +   
            "    $B = FILTER D BY ($1 == 8) OR (NOT ($0+$2 > $1));\n" +
            "};\n";
        
        String script = 
            "delta = group_and_count();\n" +
            "store delta into 'byage';\n";
        
        String expected =
            "macro_group_and_count_D_0 = load 'myfile.txt' USING PigStorage('\t') AS (a0:int, a1:int, a2:int);\n" +
            "delta = FILTER macro_group_and_count_D_0 BY (($1 == 8) OR ( NOT ($0 + $2 > $1)));\n" +
            "store delta INTO 'byage';\n";
            
        verify(macro + script, expected);
    }
    
    @Test
    public void noReturnTest() throws Exception {
        String macro = "define group_and_count() returns void {\n" +
            "    D = load 'myfile.txt' using PigStorage() AS (a0:int, a1:int, a2:int);\n" +   
            "    store D into 'myoutput';\n" +
            "};\n";
        
        String script = 
            "group_and_count();\n";
        
        String expected =
            "macro_group_and_count_D_0 = load 'myfile.txt' USING PigStorage() AS (a0:int, a1:int, a2:int);\n" +
            "store macro_group_and_count_D_0 INTO 'myoutput';\n";
            
        verify(macro + script, expected);
    }
    
    @Test
    public void noReturnTest2() throws Exception {
        String macro = "define group_and_count(myinput, myoutput) returns void {\n" +
            "    D = load '$myinput';\n" +   
            "    store D into '$myoutput';\n" +
            "};\n";
        
        String script = 
            "group_and_count('myfile.txt', '/tmp/myoutput');\n";
        
        String expected =
            "macro_group_and_count_D_0 = load 'myfile.txt';\n" +
            "store macro_group_and_count_D_0 INTO '/tmp/myoutput';\n";
            
        verify(macro + script, expected);
    }
    
    // missing inline parameters
    @Test
    public void negativeTest() throws Throwable {
        String macro = "define group_and_count (A,group_key, size) returns B {\n" +
            "    $B = limit $A $size;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, 20);\n" +
            "store gamma into 'byuser';\n";
        
        String expectedErr = "Reason: Expected number of parameters: 3 actual number of inputs: 2";
        
        validateFailure(macro + script, expectedErr);
    }   
    
    // missing inline parameters 
    @Test 
    public void negativeTest2() throws Throwable {
        String macro = "define group_and_count (A,group_key, size) returns B {\n" +
            "    $B = limit $A $size;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count ();\n" +
            "store gamma into 'byuser';\n";
        
        String expectedErr = "Reason: Expected number of parameters: 3 actual number of inputs: 0";
        
        validateFailure(macro + script, expectedErr);
    }   
    
    // missing inline return values
    @Test
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
        
        String expectedErr = "Reason: Expected number of return aliases: 2 actual number of return values: 1";
        
        validateFailure(macro + script, expectedErr);
    }
    
    // macro contains another macro def
    @Test
    public void negativeTest4() throws Throwable {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    $B = JOIN $A BY user, $C BY user;\n" +
            "    define test_macro() returns dummy { a = load '1.txt'; };\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        String expectedErr 
            = "Reason: macro 'group_and_count' contains macro definition.\n" +
              "macro content: \n" +
              "    $B = JOIN $A BY user, $C BY user;\n" +
              "    define test_macro() returns dummy { a = load '1.txt'; };\n";
        
        validateFailure(macro + script, expectedErr);
    }
    


    // macro doesn't contain return alias
    @Test
    public void negativeTest5() throws Throwable {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    B = JOIN $A BY user, $C BY user;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        String expectedErr = "Reason: Macro 'group_and_count' missing return alias: B";
        
        validateFailure(macro + script, expectedErr);
    }
    
    // macro doesn't contain return alias
    @Test
    public void negativeTest6() throws Throwable {
        String macro = "define group_and_count (A,C) returns B {\n" +
            "    B = JOIN $A BY $B, $C BY user;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        String expectedErr = "Reason: Macro 'group_and_count' missing return alias: B";
        
        validateFailure(macro + script, expectedErr);
    }
    
    @Test // macro doesn't contain return alias
    public void checkReturnAliasTest1() throws Throwable {
        String macro = "define group_and_count_1 (A,C) returns B {\n" +
            "    /* this is a test $B and \n" +
            "    $B = JOIN $A BY user, $C BY user; */\n" +
            "    B = JOIN $A BY user, $C BY user;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count_1 (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        String expectedErr = "Reason: Macro 'group_and_count_1' missing return alias: B";
        
        validateFailure(macro + script, expectedErr);
    }
    
    @Test // macro doesn't contain return alias
    public void checkReturnAliasTest2() throws Throwable {
        String macro = "define group_and_count_2 (A,C) returns B {\n" +
            "    -- $B = JOIN $A BY user, $C BY user; \n" +
            "    --$B = JOIN $A BY user, $C BY user; \n" +
            "    B = JOIN $A BY user, $C BY user;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count_2 (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        String expectedErr = "Reason: Macro 'group_and_count_2' missing return alias: B";
        
        validateFailure(macro + script, expectedErr);
    }
    
    @Test // PIG-2088
    public void checkReturnAliasTest3() throws Throwable {
        String macro = "define group_and_count_2 (A,C) returns B {\n" +
            "    -- D = JOIN $A BY user, $C BY user; \n" +
            "    --D = JOIN $A BY user, $C BY user; \n" +
            "    $B = JOIN $A BY user, $C BY user;\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = group_and_count_2 (alpha,beta);\n" +
            "store gamma into 'byuser';\n";
        
        String expected = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = load 'links' as (user, link, view);\n" +
            "gamma = JOIN alpha BY (user), beta BY (user);\n" +
            "store gamma INTO 'byuser';\n";

        verify(macro + script, expected);
    }
    
    // PIG-1999: macro contains schema name that conflicts with alias
    @Test
    public void negativeTest7() throws Throwable {
        String macro = "define toBytearray(in, intermediate) returns e {\n"
                + "b = load '$in' as (name:chararray, age:long, gpa: float);\n"
                + "d = load '$intermediate' using BinStorage() as (b:bag{t:tuple(x,y,z)}, t2:tuple(a,b,c));\n"
                + "$e = foreach d generate COUNT(b), t2.a, t2.b, t2.c;};\n";

        String script = "f = toBytearray ('data', 'output1');\n";
        
        String expectedErr = "pig script failed to validate: Macro doesn't support user defined schema "
                + "that contains name that conflicts with alias name: b\n"
                + "macro content: \n"
                + "b = load 'data' as (name:chararray, age:long, gpa: float);\n"
                + "d = load 'output1' using BinStorage() as (b:bag{t:tuple(x,y,z)}, t2:tuple(a,b,c));\n"
                + "f = foreach d generate COUNT(b), t2.a, t2.b, t2.c;\n";

        validateFailure(macro + script, expectedErr, "pig script failed to validate:");
    }
    
    // PIG-1999: macro contains schema name that conflicts with alias
    @Test
    public void negativeTest8() throws Throwable {
        String macro = "define toBytearray(in, intermediate) returns e {\n"
                + "b = load '$in' as (name:chararray, age:long, gpa: float);\n"
                + "d = foreach b generate $0 as b:bag{t:tuple(x,y,z)};\n"
                + "$e = foreach d generate COUNT(b), t2.a, t2.b, t2.c;};\n";

        String script = "f = toBytearray ('data', 'output1');\n";
        
        String expectedErr = "pig script failed to validate: Macro doesn't support user defined schema "
                + "that contains name that conflicts with alias name: b\n"
                + "macro content: \n"
                + "b = load 'data' as (name:chararray, age:long, gpa: float);\n"
                + "d = foreach b generate $0 as b:bag{t:tuple(x,y,z)};\n"
                + "f = foreach d generate COUNT(b), t2.a, t2.b, t2.c;\n";

        validateFailure(macro + script, expectedErr, "pig script failed to validate:");
    }
   
    // PIG-2058
    @Test
    public void negativeTest9() throws Throwable {
        String macro = "define test( out1,out2 ){ A = load 'x' as (u:int, v:int); $B = filter A by u < 3 and v < 20; }";
        
        String expectedErr = "<file myscript.pig, line 1, column 24>  mismatched input '{' expecting RETURNS";

        validateFailure(macro, expectedErr, "<file");
    }
    
    @Test
    public void recursiveMacrosTest() throws Exception {
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

        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "macro_group_and_partition_C_0 = group alpha by (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n" +
            "gamma = foreach macro_group_and_partition_C_0 generate group, COUNT(alpha);\n" +
            "store gamma INTO 'byuser';\n";
            
        verify(script, expected);
    }
    
    @Test // PIG-2012
    public void lineNumberTest1() throws Throwable {
        StringBuilder sb = new StringBuilder();
        sb.append("-- Given daily input and a particular year, analyze how\n") 
            .append("-- stock prices changed on days dividends were paid out.\n");
        
        sb.append("define dividend_analysis (daily, year, daily_symbol, daily_open, daily_close)\n" +
            "returns analyzed {\n" +
            "divs          = load 'NYSE_dividends' as (exchange:chararray,\n" +
            "            symbol:chararray, date:chararray, dividends:float);\n" +
            "divsthisyear  = filter divs by date matches '$year-.*';\n" +
            "dailythisyear = filter $daily by date matches '$year-.*';\n" +
            "jnd           = join divsthisyear by symbol, dailythisyear by $daily_symbol;\n" +
            "$analyzed     = foreach jnd generate $daily_symbol, $daily_close - $daily_open;\n" +
            "};\n");
        
        sb.append("daily   = load 'NYSE_daily' as (exchange:chararray, symbol:chararray,\n" +
            "date:chararray, open:float, high:float, low:float, close:float,\n" +
            "volume:int, adj_close:float);\n" +
            "results = dividend_analysis(daily, '2009', 'symbol', 'open', 'close');\n" +
            "store results into 'output';\n");
        
        String expectedErr = "at expanding macro 'dividend_analysis' (myscript.pig:15)\n" +
            "<file myscript.pig, line 10, column 35> Invalid field projection. Projected field [symbol] " +
            "does not exist in schema:";
        
        validateFailure(sb.toString(), expectedErr, "at");
    }
    
    @Test // PIG-2012
    public void lineNumberTest2() throws Throwable {
        StringBuilder sb = new StringBuilder();
        sb.append("-- Given daily input and a particular year, analyze how\n") 
            .append("-- stock prices changed on days dividends were paid out.\n");
        
        sb.append("define dividend_analysis (daily, year, daily_symbol, daily_open, daily_close)\n" +
            "returns analyzed {\n" +
            "divs          = load 'NYSE_dividends' as (exchange:chararray,\n" +
            "            symbol:chararray, date:chararray, dividends:float);\n" +
            "divsthisyear  = filter divs by date matches '$year-.*';\n" +
            "dailythisyear = filter $daily by date matches '$year-.*';\n" +
            "jnd           = join divsthisyear by symbol, dailythisyear by $daily_symbol;\n" +
            "$analyzed     = foreach jnd generate $daily_symbol, $daily_close - $daily_open;\n" +
            "};\n");
        
        sb.append("define addition_macro (daily, year, daily_symbol, daily_open, daily_close) returns B {\n" +
            "$B = dividend_analysis($daily, '$year', '$daily_symbol', '$daily_open', '$daily_close');\n" +
            "};\n");
        
        sb.append("daily   = load 'NYSE_daily' as (exchange:chararray, symbol:chararray,\n" +
            "date:chararray, open:float, high:float, low:float, close:float,\n" +
            "volume:int, adj_close:float);\n" +
            "results = addition_macro(daily, '2009', 'symbol', 'open', 'close');\n" +
            "store results into 'output';\n");
        
        String expectedErr = 
            "at expanding macro 'addition_macro' (myscript.pig:18)\n" +
            "at expanding macro 'dividend_analysis' (myscript.pig:13)\n" +
            "<file myscript.pig, line 10, column 35> Invalid field projection. Projected field [symbol] " +
            "does not exist in schema:";
        
        validateFailure(sb.toString(), expectedErr, "at");
    }
    
    //see Pig-2184
    @Test
    public void testMacroAliasConversion() throws Exception {
          String macro = "define my_macro (X,key) returns Y {\n" +
                "$Y = filter $X by $key>0;\n" +
                    "};\n";
                
                String script = 
                    "A = load 'sometext1' using TextLoader() as (row1) ;\n" +
                    "E = my_macro (A, $0);\n" +
                    "store E into 'byrow1';\n";
                
                String expected =
                            "A = load 'sometext1' USING TextLoader() as row1;\n"+
                            "E = filter A BY ($0 > 0);\n"+
                            "store E INTO 'byrow1';\n";
                verify(macro + script, expected);
    }
    
    @Test
    public void recursiveMacrosTest3() throws Exception {
        String macro1 = "define group_and_partition (A, group_key, reducers) returns B, D  {\n" +
            "    C = group $A by $group_key partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "    $B, $D = foreach_count(C, $A);" +
            "};\n";
        
        String macro2 = "define foreach_count(A, C) returns B, D {\n" +
            "   $B = foreach $A generate group, COUNT($C);\n" +
            "   $D = foreach $A generate group, COUNT($C);\n" +
            "};\n";
        
        
        String script = macro2 + macro1 +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma, beta = group_and_partition (alpha, user, 23);\n" +
            "store beta into 'byage';\n" +  
            "store gamma into 'byuser';\n";

        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "macro_group_and_partition_C_0 = group alpha by (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n" + 
            "gamma = foreach macro_group_and_partition_C_0 generate group, COUNT(alpha);\n" +
            "beta = foreach macro_group_and_partition_C_0 generate group, COUNT(alpha);\n" +
            "store beta INTO 'byage';\n" +
            "store gamma INTO 'byuser';\n";
            
        verify(script, expected);
    }
    
    @Test
    public void recursiveMacrosTest2() throws Exception {
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

        String expected =
            "macro_load_and_group_alpha_0 = load 'users' as (user, age, zip);\n" +
            "macro_load_and_group_macro_group_and_partition_C_0_0 = group macro_load_and_group_alpha_0 by (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 30;\n" +
            "gamma = foreach macro_load_and_group_macro_group_and_partition_C_0_0 generate group, COUNT(macro_load_and_group_alpha_0);\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(script, expected);
    }
    
    @Test // PIG-2035
    public void nestedMacrosTest() throws Exception {
        String macro1 = "define test ( in, out, x ) returns void {\n" +
            "a = load '$in' as (name, age, gpa);\n" +
            "b = group a by gpa;\n" +
            "c = foreach b generate group, COUNT(a.$x);\n" +
            "store c into '$out';\n" +
            "};\n";
        
        String macro2 = "define test2( in, out ) returns void {\n" +
            "test( '$in', '$out', 'name' );\n" +
            "test( '$in', '$out.1', 'age' );\n" +
            "test( '$in', '$out.2', 'gpa' );\n" +
            "};\n";
        
        String script = "test2('studenttab10k', 'myoutput');";
        
        String expected =
            "macro_test2_macro_test_a_0_0 = load 'studenttab10k' as (name, age, gpa);\n" +
            "macro_test2_macro_test_b_0_0 = group macro_test2_macro_test_a_0_0 by (gpa);\n" +
            "macro_test2_macro_test_c_0_0 = foreach macro_test2_macro_test_b_0_0 generate group, COUNT(macro_test2_macro_test_a_0_0.(name));\n" +
            "store macro_test2_macro_test_c_0_0 INTO 'myoutput';\n" +
            "macro_test2_macro_test_a_1_0 = load 'studenttab10k' as (name, age, gpa);\n" +
            "macro_test2_macro_test_b_1_0 = group macro_test2_macro_test_a_1_0 by (gpa);\n" +
            "macro_test2_macro_test_c_1_0 = foreach macro_test2_macro_test_b_1_0 generate group, COUNT(macro_test2_macro_test_a_1_0.(age));\n" +
            "store macro_test2_macro_test_c_1_0 INTO 'myoutput.1';\n" +
            "macro_test2_macro_test_a_2_0 = load 'studenttab10k' as (name, age, gpa);\n" +
            "macro_test2_macro_test_b_2_0 = group macro_test2_macro_test_a_2_0 by (gpa);\n" +
            "macro_test2_macro_test_c_2_0 = foreach macro_test2_macro_test_b_2_0 generate group, COUNT(macro_test2_macro_test_a_2_0.(gpa));\n" +
            "store macro_test2_macro_test_c_2_0 INTO 'myoutput.2';\n";
            
        verify(macro1 + macro2 + script, expected);
    }
    
    @Test
    public void sequenceMacrosTest() throws Exception {
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

        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "beta = group alpha by (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 20;\n" +
            "gamma = foreach beta generate group, COUNT(alpha);\n" +
            "store gamma INTO 'byuser';\n";
            
        verify(script, expected);
    }
    
    @Test
    public void selfRecursiveTest() throws Throwable {
        String macro1 = "define group_and_partition (A, group_key, reducers) returns B {\n" +
            "    C = group $A by $group_key partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel $reducers;\n" +
            "    $B = group_and_partition(C, age, 34);" +
            "};\n";
        
        String script = macro1 +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_partition (alpha, user, 23);\n" +
            "store gamma into 'byuser';\n";

        String expectedErr = "Reason: Macro can't be defined circularly.";
        
        validateFailure(script, expectedErr);
    }
    
    @Test
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

        String expectedErr = "Reason: Macro can't be defined circularly.";
        
        validateFailure(script, expectedErr);
    }
    
    @Test
    public void typecastTest() throws Exception {
        String macro = 
            "a = load '1.txt' as (a0);" +
            "b = foreach a generate flatten( (bag{tuple(map[])})a0 ) as b0:map[];" +
            "c = foreach b generate (long)b0#'key1';";
        
        String expected =
            "macro_mymacro_a_0 = load '1.txt' as a0;\n" +
            "macro_mymacro_b_0 = foreach macro_mymacro_a_0 generate flatten((bag{tuple(map[])})a0)  AS b0:map[];\n" +
            "macro_mymacro_c_0 = foreach macro_mymacro_b_0 generate (long)b0#'key1';\n";
            
        testMacro(macro, expected);
    }
    
    @Test // PIG-2005
    public void noSemiColonTest() throws Exception {
        String macro = "define group_and_count (A) returns B {\n" +
            "    $B = FILTER $A BY ($1 == 8) OR (NOT ($0+$2 > $1));\n" +
            "}\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha);\n" +
            "store gamma into 'byuser';\n";
        
        String expected = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = FILTER alpha BY (($1 == 8) OR ( NOT ($0 + $2 > $1)));\n" +
            "store gamma INTO 'byuser';\n";
        
        verify(macro + script, expected);
    }

    @Test // PIG-2887
    public void testNegativeNumber() throws Exception {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " +
                       "B = filter A by 2 > -1; " ;

        String expected =
            "macro_mymacro_A_0 = load 'x' as (u:int, v:long, w:bytearray);\n" +
            "macro_mymacro_B_0 = filter macro_mymacro_A_0 BY (2 > -1);\n" ;
        testMacro( query, expected );
    }
    
    @Test
    public void test1() throws Exception {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = limit A 100; " +
                       "C = filter B by 2 > 1; " +
                       "D = load 'y' as (d1, d2); " +
                       "E = join C by ( $0, $1 ), D by ( d1, d2 ) using 'replicated' parallel 16; " +
                       "F = store E into 'output';";
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (u:int, v:long, w:bytearray);\n" +
            "macro_mymacro_B_0 = limit macro_mymacro_A_0 100;\n" +
            "macro_mymacro_C_0 = filter macro_mymacro_B_0 BY (2 > 1);\n" +
            "macro_mymacro_D_0 = load 'y' as (d1, d2);\n" +
            "macro_mymacro_E_0 = join macro_mymacro_C_0 by ($0, $1), macro_mymacro_D_0 by (d1, d2) USING 'replicated' parallel 16;\n" +
            "macro_mymacro_F_0 = store macro_mymacro_E_0 INTO 'output';\n";
            
        testMacro( query, expected );

    }

    @Test
    public void test2() throws Exception {
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
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (u:int, v:long, w:bytearray);\n" +
            "macro_mymacro_B_0 = distinct macro_mymacro_A_0 partition BY org.apache.pig.Identity;\n" +
            "macro_mymacro_C_0 = sample macro_mymacro_B_0 0.49;\n" +
            "macro_mymacro_D_0 = order macro_mymacro_C_0 BY $0, $1;\n" +
            "macro_mymacro_E_0 = load 'y' as (d1, d2);\n" + 
            "macro_mymacro_F_0 = union onschema macro_mymacro_D_0, macro_mymacro_E_0;\n" +
            "macro_mymacro_G_0 = load 'z' as (g1:int, g2:(g21, g22));\n" +
            "macro_mymacro_H_0 = cross macro_mymacro_F_0, macro_mymacro_G_0;\n" +
            "split macro_mymacro_H_0 INTO macro_mymacro_I_0 IF 10 > 5, macro_mymacro_J_0 IF 'world' eq 'hello', macro_mymacro_K_0 IF 77 <= 200;\n" + 
            "macro_mymacro_L_0 = store macro_mymacro_J_0 INTO 'output';\n";
        
        testMacro( query, expected );
    }

    @Test
    public void test3() throws Exception {
        String query = "a = load '1.txt'  as (name, age, gpa);" + 
                       "b = group a by name PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner2;" +
                       "c = foreach b generate group, COUNT(a.age);" +
                       "store c into 'y';";
        
        String expected =
            "macro_mymacro_a_0 = load '1.txt' as (name, age, gpa);\n" +
            "macro_mymacro_b_0 = group macro_mymacro_a_0 by (name) PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner2;\n" +
            "macro_mymacro_c_0 = foreach macro_mymacro_b_0 generate group, COUNT(macro_mymacro_a_0.(age));\n" +
            "store macro_mymacro_c_0 INTO 'y';\n";
            
        testMacro( query, expected );
    }

    @Test
    public void test4() throws Exception {
        String query = "A = load 'x'; " + 
                       "B = mapreduce '" + "myjar.jar" + "' " +
                           "Store A into 'table_testNativeMRJobSimple_input' "+
                           "Load 'table_testNativeMRJobSimple_output' "+
                           "`org.apache.pig.test.utils.WordCount -files " + "file " +
                           "table_testNativeMRJobSimple_input table_testNativeMRJobSimple_output " +
                           "stopworld.file" + "`;" +
                        "C = Store B into 'output';";
        
        String expected =
            "macro_mymacro_A_0 = load 'x';\n" + 
            "macro_mymacro_B_0 = mapreduce 'myjar.jar' Store macro_mymacro_A_0 INTO 'table_testNativeMRJobSimple_input' Load 'table_testNativeMRJobSimple_output' `org.apache.pig.test.utils.WordCount -files file table_testNativeMRJobSimple_input table_testNativeMRJobSimple_output stopworld.file`;\n" +
            "macro_mymacro_C_0 = Store macro_mymacro_B_0 INTO 'output';\n";
            
        testMacro( query, expected );
    }

    // Test define function.
    @Test
    public void test5() throws Exception {
        String query = "define myudf org.apache.pig.builtin.PigStorage( ',' );" +
                       "A = load 'x' using myudf;" +
                       "store A into 'y';";
        
        String expected =
            "define myudf org.apache.pig.builtin.PigStorage(',');\n" +
            "macro_mymacro_A_0 = load 'x' USING myudf;\n" +
            "store macro_mymacro_A_0 INTO 'y';\n";
            
        testMacro( query, expected );
    }

    @Test
    public void test6() throws Exception {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = group A by ( a, $2 );" +
                       "store B into 'y';";
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (a:int, b, c:chararray);\n" +
            "macro_mymacro_B_0 = group macro_mymacro_A_0 by (a, $2);\n" + 
            "store macro_mymacro_B_0 INTO 'y';\n";
        
        testMacro( query, expected );
    }

    @Test
    public void test7() throws Exception {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = foreach A generate a, $2;" +
                       "store B into 'y';";
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (a:int, b, c:chararray);\n" +
            "macro_mymacro_B_0 = foreach macro_mymacro_A_0 generate a, $2;\n" +
            "store macro_mymacro_B_0 INTO 'y';\n";
            
        testMacro( query, expected );
    }

    @Test
    public void test8() throws Exception {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = group A by a;" +
                       "C = foreach B { S = A.b; generate S; };" +
                       "store C into 'y';";
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (a:int, b, c:chararray);\n" +
            "macro_mymacro_B_0 = group macro_mymacro_A_0 by (a);\n" + 
            "macro_mymacro_C_0 = foreach macro_mymacro_B_0 { S = macro_mymacro_A_0.(b);  generate S; } ;\n" +
            "store macro_mymacro_C_0 INTO 'y';\n";
            
        testMacro( query, expected );
    }
    
    @Test
    public void test9() throws Exception {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { R = a; S = R.u; T = limit S 100; generate S, T, c + d/5; };" +
                       "store B into 'y';";
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (a:bag{T:(u, v)}, c:int, d:long);\n" +
            "macro_mymacro_B_0 = foreach macro_mymacro_A_0 { R = a; S = R.(u); T = limit S 100;  generate S, T, c + d / 5; } ;\n" +
            "store macro_mymacro_B_0 INTO 'y';\n";
            
        testMacro( query, expected );
    }

    @Test
    public void test10() throws Exception {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { S = a; T = limit S 100; generate T; };" +
                       "store B into 'y';";
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (a:bag{T:(u, v)}, c:int, d:long);\n" +
            "macro_mymacro_B_0 = foreach macro_mymacro_A_0 { S = a; T = limit S 100;  generate T; } ;\n" +
            "store macro_mymacro_B_0 INTO 'y';\n";
            
        testMacro( query, expected );
    }

    @Test
    public void test11() throws Exception {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { T = limit a 100; generate T; };" +
                       "store B into 'y';";
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (a:bag{T:(u, v)}, c:int, d:long);\n" +
            "macro_mymacro_B_0 = foreach macro_mymacro_A_0 { T = limit a 100;  generate T; } ;\n" +
            "store macro_mymacro_B_0 INTO 'y';\n";
            
        testMacro( query, expected );
    }

    @Test
    public void test12() throws Exception {
        String query = "define CMD `perl GroupBy.pl '\t' 0 1` ship('"+Util.encodeEscape(command.toString())+"');" +
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
    
    @Test
    public void test13() throws Exception {
        String query = "define CMD `perl PigStreaming.pl` ship('"+Util.encodeEscape(command.toString())+"') stderr('CMD');" +
                       "A = load '/user/pig/tests/data/singlefile/studenttab10k';" +
                       "C = stream A through CMD;" +
                       "store C into '/user/pig/out/jianyong.1297238871/StreamingPerformance_1.out';";
        testMacro( query );
    }
    
    @Test
    public void test14() throws Exception {
        String query = "a = load '/user/pig/tests/data/singlefile/studenttab10k' using PigStorage() as (name, age:int, gpa);" +
                       "b = load '/user/pig/tests/data/singlefile/votertab10k' as (name, age, registration, contributions);" +
                       "e = cogroup a by name, b by name parallel 8;" +
                       "f = foreach e generate group,  SUM(a.age) as s;" +
                       "g = filter f by s>0;" +
                       "store g into '/user/pig/out/jianyong.1297323675/Accumulator_1.out';";
        
        String expected =
            "macro_mymacro_a_0 = load '/user/pig/tests/data/singlefile/studenttab10k' USING PigStorage() as (name, age:int, gpa);\n" +
            "macro_mymacro_b_0 = load '/user/pig/tests/data/singlefile/votertab10k' as (name, age, registration, contributions);\n" +
            "macro_mymacro_e_0 = cogroup macro_mymacro_a_0 by (name), macro_mymacro_b_0 by (name) parallel 8;\n" +
            "macro_mymacro_f_0 = foreach macro_mymacro_e_0 generate group, SUM(macro_mymacro_a_0.(age)) AS s;\n" +
            "macro_mymacro_g_0 = filter macro_mymacro_f_0 BY (s > 0);\n" +
            "store macro_mymacro_g_0 INTO '/user/pig/out/jianyong.1297323675/Accumulator_1.out';\n";
        
        testMacro( query, expected );
    }
    
    @Test
    public void test15() throws Exception {
        String query = "a = load '/user/pig/tests/data/singlefile/studenttab10k' using PigStorage() as (name, age, gpa);" +
                       "b = group a all;" +
                       "c = foreach b generate AVG(a.age) as avg; " +
                       "d = load '/user/pig/tests/data/singlefile/votertab10k' using PigStorage() as (name, age, registration, contributions);" +
                       "e = group d all;" +
                       "f = foreach e generate AVG(d.age) as avg;" +
                       "y = foreach a generate age/c.avg, age/f.avg;" +
                       "store y into '/user/pig/out/jianyong.1297323675/Scalar_4.out';";
        
        String expected =
            "macro_mymacro_a_0 = load '/user/pig/tests/data/singlefile/studenttab10k' USING PigStorage() as (name, age, gpa);\n" +
            "macro_mymacro_b_0 = group macro_mymacro_a_0 all;\n" + 
            "macro_mymacro_c_0 = foreach macro_mymacro_b_0 generate AVG(macro_mymacro_a_0.(age)) AS avg;\n" + 
            "macro_mymacro_d_0 = load '/user/pig/tests/data/singlefile/votertab10k' USING PigStorage() as (name, age, registration, contributions);\n" +
            "macro_mymacro_e_0 = group macro_mymacro_d_0 all;\n" + 
            "macro_mymacro_f_0 = foreach macro_mymacro_e_0 generate AVG(macro_mymacro_d_0.(age)) AS avg;\n" +
            "macro_mymacro_y_0 = foreach macro_mymacro_a_0 generate age / macro_mymacro_c_0.(avg), age / macro_mymacro_f_0.(avg);\n" +
            "store macro_mymacro_y_0 INTO '/user/pig/out/jianyong.1297323675/Scalar_4.out';\n";
        
        testMacro( query, expected );
    }
    
    @Test
    public void test16() throws Exception {
        String query = "AA = load '/user/pig/tests/data/singlefile/studenttab10k';" +
                       "A = foreach (group (filter AA by $0 > 0) all) generate flatten($1);" +
                       "store A into '/user/pig/out/jianyong.1297323675/Scalar_4.out';";
        
        String expected =
            "macro_mymacro_AA_0 = load '/user/pig/tests/data/singlefile/studenttab10k';\n" +
            "macro_mymacro_A_0 = foreach  (group  (filter macro_mymacro_AA_0 BY ($0 > 0))  all)  generate flatten($1) ;\n" +
            "store macro_mymacro_A_0 INTO '/user/pig/out/jianyong.1297323675/Scalar_4.out';\n";
            
        testMacro( query, expected );
    }

    
    @Test
    public void test17() throws Exception {
        String macro = 
            "a = load '/user/pig/tests/data/singlefile/studentnulltab10k' as (name:chararray, age:int, gpa:double);" +
            "b = foreach a generate (int)((int)gpa/((int)gpa - 1)) as norm_gpa:int;" + 
            "c = foreach b generate (norm_gpa is not null? norm_gpa: 0);" +
            "store c into '/user/pig/out/jianyong.1297229709/Types_37.out';";
            
        String expected =
            "macro_mymacro_a_0 = load '/user/pig/tests/data/singlefile/studentnulltab10k' as (name:chararray, age:int, gpa:double);\n" +
            "macro_mymacro_b_0 = foreach macro_mymacro_a_0 generate (int)((int)gpa / ((int)gpa - 1)) AS norm_gpa:int;\n" +
            "macro_mymacro_c_0 = foreach macro_mymacro_b_0 generate  (norm_gpa IS not null ? norm_gpa : 0) ;\n" +
            "store macro_mymacro_c_0 INTO '/user/pig/out/jianyong.1297229709/Types_37.out';\n";
            
        testMacro(macro, expected);
    }
    
    @Test
    public void test18() throws Exception {
        String macro = "define mymacro() returns void {" +
            "a = load '/user/pig/tests/data/singlefile/studenttab10k';" +
            "b = group a by $0;" +
            "c = foreach b {c1 = order $1 by *; generate flatten(c1); };" +
            "store c into '/user/pig/out/jianyong.1297305352/Order_15.out';};";
            
        String script = macro +
            "mymacro();\n";
        
        String expected =
            "macro_mymacro_a_0 = load '/user/pig/tests/data/singlefile/studenttab10k';\n" +
            "macro_mymacro_b_0 = group macro_mymacro_a_0 by ($0);\n" +
            "macro_mymacro_c_0 = foreach macro_mymacro_b_0 { c1 = order $1 BY *;  generate flatten(c1) ; } ;\n" +
            "store macro_mymacro_c_0 INTO '/user/pig/out/jianyong.1297305352/Order_15.out';\n";
        
        verify(script, expected);
    }
    
    @Test
    public void test19() throws Exception {
        String macro = 
            "a = load '/user/pig/tests/data/singlefile/studenttab10k';" +
            "b = group a by $0;" +
            "c = foreach b {c1 = order $1 by $1; generate flatten(c1), MAX($1.$1); };" +
            "store c into '/user/pig/out/jianyong.1297305352/Order_17.out';";
            
        String expected =
            "macro_mymacro_a_0 = load '/user/pig/tests/data/singlefile/studenttab10k';\n" +
            "macro_mymacro_b_0 = group macro_mymacro_a_0 by ($0);\n" +
            "macro_mymacro_c_0 = foreach macro_mymacro_b_0 { c1 = order $1 BY $1;  generate flatten(c1) , MAX($1.($1)); } ;\n" +
            "store macro_mymacro_c_0 INTO '/user/pig/out/jianyong.1297305352/Order_17.out';\n";
            
        testMacro(macro, expected);
    }
    
    @Test
    public void test20() throws Exception {
        String macro = 
            "a = load 'x' as (u,v);" +
            "b = load 'y' as (u,w);" +
            "c = join a by u, b by u;" +
            "d = foreach c generate a::u, b::u, w;";
            
        String expected =
            "macro_mymacro_a_0 = load 'x' as (u, v);\n" +
            "macro_mymacro_b_0 = load 'y' as (u, w);\n" +
            "macro_mymacro_c_0 = join macro_mymacro_a_0 by (u), macro_mymacro_b_0 by (u);\n" +
            "macro_mymacro_d_0 = foreach macro_mymacro_c_0 generate macro_mymacro_a_0::u, macro_mymacro_b_0::u, w;\n";
        
        testMacro(macro, expected);
    }
    
    @Test
    public void test21() throws Exception {
        String macro = 
            "a = load '1.txt' as ( u:tuple(p, q), v, w : int );" +
            "b = foreach a generate * as ( x, y, z ), flatten( u ) as ( r, s ), flatten( v ) as d, w + 5 as e:int;";
            
        String expected =
            "macro_mymacro_a_0 = load '1.txt' as (u:(p, q), v, w:int);\n" +
            "macro_mymacro_b_0 = foreach macro_mymacro_a_0 generate  * AS (x, y, z), flatten(u)  AS (r, s), flatten(v)  AS d, w + 5 AS e:int;\n";
        
        testMacro(macro, expected);
    }
    
    @Test
    public void test22() throws Exception {
        String macro =
            "a = load '1.txt' as ( u : bag{}, v : bag{tuple(x, y)} );" +
            "b = load '2.x' as ( t : {}, u : {(r,s)}, v : bag{ T : tuple( x, y ) }, w : bag{(z1, z2)} );" +
            "c = load '3.x' as p : int;";
            
        String expected =
            "macro_mymacro_a_0 = load '1.txt' as (u:bag{}, v:bag{T:(x, y)});\n" +
            "macro_mymacro_b_0 = load '2.x' as (t:bag{}, u:bag{T:(r, s)}, v:bag{T:(x, y)}, w:bag{T:(z1, z2)});\n" +
            "macro_mymacro_c_0 = load '3.x' as p:int;\n";
        
        testMacro(macro, expected);
    }

    @Test
    public void test23() throws Exception {
        String macro = 
            "a = load '1.txt' as ( u:tuple(p, q), v, w : int );" +
            "b = foreach a generate * as ( x, y, z ), flatten( u ) as ( r, s ), flatten( v ) as d, w + 5 as e:int;";
            
        String expected =
            "macro_mymacro_a_0 = load '1.txt' as (u:(p, q), v, w:int);\n" +
            "macro_mymacro_b_0 = foreach macro_mymacro_a_0 generate  * AS (x, y, z), flatten(u)  AS (r, s), flatten(v)  AS d, w + 5 AS e:int;\n";
        
        testMacro(macro, expected);
    }
    
    @Test
    public void test24() throws Exception {
        String macro = 
            "A = load 'x' as ( u:bag{tuple(x, y)}, v:long, w:bytearray); " + 
            "B = foreach A generate u.(x, y), v, w; " +
            "C = store B into 'output';";
            
        String expected =
            "macro_mymacro_A_0 = load 'x' as (u:bag{T:(x, y)}, v:long, w:bytearray);\n" +
            "macro_mymacro_B_0 = foreach macro_mymacro_A_0 generate u.(x, y), v, w;\n" +
            "macro_mymacro_C_0 = store macro_mymacro_B_0 INTO 'output';\n";
        
        testMacro(macro, expected);
    }
    
    @Test
    public void test25() throws Exception {
        String macro = 
            "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
            "B = foreach A { R = a; P = c * 2; Q = P + d; S = R.u; T = limit S 100; generate Q, R, S, T, c + d/5; };" +
            "store B into 'y';";
            
        String expected =
            "macro_mymacro_A_0 = load 'x' as (a:bag{T:(u, v)}, c:int, d:long);\n" +
            "macro_mymacro_B_0 = foreach macro_mymacro_A_0 { R = a; P = c * 2; Q = P + d; S = R.(u); T = limit S 100;  generate Q, R, S, T, c + d / 5; } ;\n" +
            "store macro_mymacro_B_0 INTO 'y';\n";
        
        testMacro(macro, expected);
    }
    
    @Test
    public void test26() throws Exception {
        String content =
            "A = load 'x' as ( u:bag{tuple(x, y)}, v:long, w:bytearray); " + 
            "B = foreach A generate u.(x, $1), $1, w; " +
            "C = store B into 'output';";
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (u:bag{T:(x, y)}, v:long, w:bytearray);\n" +
            "macro_mymacro_B_0 = foreach macro_mymacro_A_0 generate u.(x, $1), $1, w;\n" +
            "macro_mymacro_C_0 = store macro_mymacro_B_0 INTO 'output';\n";
            
        testMacro(content, expected);
    }
    
    @Test
    public void test27() throws Exception {
        String content =
            "A = load 'x' as ( u:bag{} ); " + 
            "B = foreach A generate u.$100; " +
            "C = store B into 'output';";
            
        String expected =
            "macro_mymacro_A_0 = load 'x' as u:bag{};\n" +
            "macro_mymacro_B_0 = foreach macro_mymacro_A_0 generate u.($100);\n" +
            "macro_mymacro_C_0 = store macro_mymacro_B_0 INTO 'output';\n";
        
        testMacro(content, expected);
    }
    
    @Test
    public void test28() throws Exception {
        String content =
            "A = load 'x'; " + 
            "B = foreach A generate $1, $1000; " +
            "C = store B into 'output';";
            
        String expected =
            "macro_mymacro_A_0 = load 'x';\n" + 
            "macro_mymacro_B_0 = foreach macro_mymacro_A_0 generate $1, $1000;\n" +
            "macro_mymacro_C_0 = store macro_mymacro_B_0 INTO 'output';\n";
        
        testMacro(content, expected);
    }
    
    @Test
    public void test29() throws Exception {
        String content =
            "A = load 'x'; " + 
            "B = load 'y' as ( u : int, v : chararray );" +
            "C = foreach A generate B.$1, $0; " +
            "D = store C into 'output';";
            
        String expected =
            "macro_mymacro_A_0 = load 'x';\n" + 
            "macro_mymacro_B_0 = load 'y' as (u:int, v:chararray);\n" +
            "macro_mymacro_C_0 = foreach macro_mymacro_A_0 generate macro_mymacro_B_0.($1), $0;\n" +
            "macro_mymacro_D_0 = store macro_mymacro_C_0 INTO 'output';\n";
        
        testMacro(content, expected);
    }
    
    @Test
    public void test30() throws Exception {
        String content =
            "A = load 'x'; " + 
            "B = load 'y' as ( u : int, v : chararray );" +
            "C = foreach A generate B.$1, $0; " +
            "D = store C into 'output';";
            
        String expected =
            "macro_mymacro_A_0 = load 'x';\n" +
            "macro_mymacro_B_0 = load 'y' as (u:int, v:chararray);\n" +
            "macro_mymacro_C_0 = foreach macro_mymacro_A_0 generate macro_mymacro_B_0.($1), $0;\n" +
            "macro_mymacro_D_0 = store macro_mymacro_C_0 INTO 'output';\n";
        
        testMacro(content, expected);
    }
    
    @Test
    public void test31() throws Exception {
        String content =
            "A = load 'x'; " + 
            "B = load 'y' as ( u : int, v : chararray );" +
            "C = foreach A generate B.v, $0; " +
            "D = store C into 'output';";
            
        String expected =
            "macro_mymacro_A_0 = load 'x';\n" +
            "macro_mymacro_B_0 = load 'y' as (u:int, v:chararray);\n" +
            "macro_mymacro_C_0 = foreach macro_mymacro_A_0 generate macro_mymacro_B_0.(v), $0;\n" +
            "macro_mymacro_D_0 = store macro_mymacro_C_0 INTO 'output';\n";
        
        testMacro(content, expected);
    }
    
    @Test
    public void testFilter() throws Exception {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = filter A by 2 > 1; ";
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (u:int, v:long, w:bytearray);\n" +
            "macro_mymacro_B_0 = filter macro_mymacro_A_0 BY (2 > 1);\n";
        
        testMacro( query, expected );
    }

    @Test
    public void testScopedAlias() throws Exception {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray);" + 
                       "B = load 'y' as ( u:int, x:int, y:chararray);" +
                       "C = join A by u, B by u;" +
                       "D = foreach C generate A::u, B::u, v, x;" +
                       "store D into 'z';";
        
        String expected =
            "macro_mymacro_A_0 = load 'x' as (u:int, v:long, w:bytearray);\n" +
            "macro_mymacro_B_0 = load 'y' as (u:int, x:int, y:chararray);\n" +
            "macro_mymacro_C_0 = join macro_mymacro_A_0 by (u), macro_mymacro_B_0 by (u);\n" +
            "macro_mymacro_D_0 = foreach macro_mymacro_C_0 generate macro_mymacro_A_0::u, macro_mymacro_B_0::u, v, x;\n" +
            "store macro_mymacro_D_0 INTO 'z';\n";
        
        testMacro ( query, expected );
    }

    @Test
    public void test32() throws Exception {
        String query = "a = load 'testSimpleMapKeyLookup' as (m:map[int]);" + 
                       "b = foreach a generate m#'key';";
        
        String expected =
            "macro_mymacro_a_0 = load 'testSimpleMapKeyLookup' as m:map[int];\n" +
            "macro_mymacro_b_0 = foreach macro_mymacro_a_0 generate m#'key';\n";
        
        testMacro( query, expected );
    }
    
    @Test
    public void test33() throws Exception {
        String query = "a = load 'testSimpleMapCast' as (m);" + 
                       "b = foreach a generate ([int])m;";
        
        String expected =
            "macro_mymacro_a_0 = load 'testSimpleMapCast' as m;\n" +
            "macro_mymacro_b_0 = foreach macro_mymacro_a_0 generate (map[int])m;\n";
        
        testMacro( query, expected );
    }
    
    @Test
    public void test34() throws Exception {
        String query = "a = load 'testComplexLoad' as (m:map[bag{(i:int,j:int)}]);";
        
        String expected =
            "macro_mymacro_a_0 = load 'testComplexLoad' as m:map[bag{T:(i:int, j:int)}];\n";
        
        testMacro( query, expected );
    }
    
    @Test
    public void test35() throws Exception {
        String query = "a = load 'testComplexCast' as (m);" +
                       "b = foreach a generate ([{(i:int,j:int)}])m;";
        
        String expected =
            "macro_mymacro_a_0 = load 'testComplexCast' as m;\n" +
            "macro_mymacro_b_0 = foreach macro_mymacro_a_0 generate (map[bag{T:(i:int, j:int)}])m;\n";
        
        testMacro( query, expected );
    }
    
    // PIG-1988
    @Test
    public void test36() throws Exception {
        createFile("mymacro.pig", " ");

        String query = "import 'mymacro.pig';" +
            "define macro1() returns void {}; " + 
            "A = load '1.txt' as (a0:int, a1:chararray);" +
            "macro1();" +
            "B = group A by a0;" + 
            "store B into 'output';";
        
        String expected = 
            "A = load '1.txt' as (a0:int, a1:chararray);\n" +
            "B = group A by (a0);\n" +
            "store B INTO 'output';\n";
        
        verify(query, expected);
    }
    
    // PIG-1987
    @Test
    public void test37() throws Exception {
        createFile("mytest.pig", groupAndCountMacro);

        String script =
            "set default_parallel 10\n" +
            "import 'mytest.pig';\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "ls\n" +
            "gamma = group_and_count (alpha, user, 23);\n" +
            "fs -copyFromLocal test test2\n" +
            "store gamma into 'byuser';\n";
        
        String expected =
            "set default_parallel 10\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "ls\n" +
            "gamma = distinct alpha partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 23;\n" +
            "fs -copyFromLocal test test2\n" +
            "store gamma INTO 'byuser';\n";
            
        verify(script, expected);
    }
    

    
    @Test
    public void testCommentInMacro() throws Exception {
        String query = "a = load 'testComplexCast' as (m);\n" +
                       " /* this is a test } and \n" +
                       " and test ***/\n" +
                       " -- this is another test } \n" +
                       "b = foreach a generate ([{(i:int,j:int)}])m;";
        
        String expected =
            "macro_mymacro_a_0 = load 'testComplexCast' as m;\n" +
            "macro_mymacro_b_0 = foreach macro_mymacro_a_0 generate (map[bag{T:(i:int, j:int)}])m;\n";

        testMacro( query, expected );
    }
    
    @Test 
    public void caseInsensitiveTest() throws Exception {
        String macro = "DEFINE group_and_count (A,group_key) RETURNS B {\n" +
            "    D = group $A by $group_key partition by org.apache.pig.test.utils.SimpleCustomPartitioner parallel 50;\n" +
            "    $B = foreach D generate group, COUNT($A);\n" +
            "};\n";
        
        String script = 
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user);\n" +
            "delta = group_and_count (alpha, age);\n" +
            "store gamma into 'byuser';\n" +
            "store delta into 'byage';\n";
                
        String expected =
            "alpha = load 'users' as (user, age, zip);\n" +
            "macro_group_and_count_D_0 = group alpha by (user) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 50;\n" +
            "gamma = foreach macro_group_and_count_D_0 generate group, COUNT(alpha);\n" +
            "macro_group_and_count_D_1 = group alpha by (age) partition BY org.apache.pig.test.utils.SimpleCustomPartitioner parallel 50;\n" +
            "delta = foreach macro_group_and_count_D_1 generate group, COUNT(alpha);\n" +
            "store gamma INTO 'byuser';\n" +
            "store delta INTO 'byage';\n";
        
        verify(macro + script, expected);
    }

    // Test for PIG-3359
    // Registers in a macro file used to fail, not being recognized by the parser
    @Test
    public void testRegister() throws Exception {
        String udfs =
            "@outputSchema(\"result: int\")\n" +
            "def some_udf(number):\n" +
            "    return number + 1\n" +
            "\n";
        createFile("my_udfs.py", udfs);

        String macro =
            "REGISTER 'my_udfs.py' USING jython AS macro_udfs;\n" +
            "DEFINE ApplyUDF(numbers) RETURNS result {\n" +
            "    $result = FOREACH $numbers GENERATE macro_udfs.some_udf($0);\n" +
            "};";
        createFile("my_macro.pig", macro);

        String script =
            "IMPORT 'my_macro.pig';\n" +
            "data = LOAD '1234.txt' USING PigStorage() AS (i: int);\n" +
            "result = ApplyUDF(data);\n" + 
            "STORE result INTO 'result.out' USING PigStorage();";

        String expected =
            "REGISTER 'my_udfs.py' USING jython AS macro_udfs;\n" +
            "data = LOAD '1234.txt' USING PigStorage() AS i:int;\n" +
            "result = FOREACH data GENERATE macro_udfs.some_udf($0);\n" + 
            "STORE result INTO 'result.out' USING PigStorage();\n";

        verify(script, expected);
    }

    // Test for PIG-3359
    @Test
    public void testParamPassedToMacroInPigscript() throws Exception {
        String macro =
            "DEFINE MultiplyMacro(numbers) RETURNS multiplied {\n" +
            "    $multiplied = FOREACH $numbers GENERATE $0 * $MULTIPLIER;\n" +
            "};";

        String script =
            "%default MULTIPLIER 5\n" +
            "data = LOAD '1234.txt' USING PigStorage() AS (i: int);\n" +
            "mult = MultiplyMacro(data);\n" + 
            "STORE mult INTO 'multiplied.out' USING PigStorage();";

        String expected =
            "data = LOAD '1234.txt' USING PigStorage() AS i:int;\n" +
            "mult = FOREACH data GENERATE $0 * 5;\n" + 
            "STORE mult INTO 'multiplied.out' USING PigStorage();\n";

        verify(macro + script, expected);
    }

    // Test for PIG-3359
    @Test
    public void testParamPassedToMacroInSeparateFile() throws Exception {
        String macro =
            "DEFINE MultiplyMacro(numbers) RETURNS multiplied {\n" +
            "    $multiplied = FOREACH $numbers GENERATE $0 * $MULTIPLIER;\n" +
            "};";
        createFile("my_macro.pig", macro);

        String script =
            "%default MULTIPLIER 5\n" +
            "IMPORT 'my_macro.pig';\n" +
            "data = LOAD '1234.txt' USING PigStorage() AS (i: int);\n" +
            "mult = MultiplyMacro(data);\n" + 
            "STORE mult INTO 'multiplied.out' USING PigStorage();";

        String expected =
            "data = LOAD '1234.txt' USING PigStorage() AS i:int;\n" +
            "mult = FOREACH data GENERATE $0 * 5;\n" + 
            "STORE mult INTO 'multiplied.out' USING PigStorage();\n";

        verify(script, expected);
    }

    // Test for PIG-3359
    @Test
    public void testDefaultInMacroFile() throws Exception {
        String macro =
            "%default MULTIPLIER 5\n" +
            "DEFINE MultiplyMacro(numbers) RETURNS multiplied {\n" +
            "    $multiplied = FOREACH $numbers GENERATE $0 * $MULTIPLIER;\n" +
            "};";
        createFile("my_macro.pig", macro);

        String script =
            "IMPORT 'my_macro.pig';\n" +
            "data = LOAD '1234.txt' USING PigStorage() AS (i: int);\n" +
            "mult = MultiplyMacro(data);\n" + 
            "STORE mult INTO 'multiplied.out' USING PigStorage();";

        String expected =
            "data = LOAD '1234.txt' USING PigStorage() AS i:int;\n" +
            "mult = FOREACH data GENERATE $0 * 5;\n" + 
            "STORE mult INTO 'multiplied.out' USING PigStorage();\n";

        verify(script, expected);
    }

    // Test for PIG-3359
    @Test
    public void testDeclareInMacroFile() throws Exception {
        String macro;
        if(Util.WINDOWS){
            macro =
                "%declare ECHOED_MULTIPLIER `echo $MULTIPLIER`" +
                "DEFINE MultiplyMacro(numbers) RETURNS multiplied {\n" +
                "    $multiplied = FOREACH $numbers GENERATE $0 * $ECHOED_MULTIPLIER;\n" +
                "};";
        } else {
             macro =
                "%declare ECHOED_MULTIPLIER `echo \"$MULTIPLIER\"`" +
                "DEFINE MultiplyMacro(numbers) RETURNS multiplied {\n" +
                "    $multiplied = FOREACH $numbers GENERATE $0 * $ECHOED_MULTIPLIER;\n" +
                "};";
        }
        createFile("my_macro.pig", macro);

        String script =
            "%default MULTIPLIER 5\n" +
            "IMPORT 'my_macro.pig';\n" +
            "data = LOAD '1234.txt' USING PigStorage() AS (i: int);\n" +
            "mult = MultiplyMacro(data);\n" + 
            "STORE mult INTO 'multiplied.out' USING PigStorage();";

        String expected =
            "data = LOAD '1234.txt' USING PigStorage() AS i:int;\n" +
            "mult = FOREACH data GENERATE $0 * 5;\n" + 
            "STORE mult INTO 'multiplied.out' USING PigStorage();\n";

        verify(script, expected);
    }

    // Test for PIG-3359
    // Macro imports used to fail if Pigscript P imported macros A and B, while A also imported B
    @Test
    public void testNestedImport() throws Exception {
        String nested_macros =
            "DEFINE NestedMultiplyMacro(numbers) RETURNS multiplied {\n" +
            "    $multiplied = FOREACH $numbers GENERATE $0 * $MULTIPLIER;\n" +
            "};\n" +
            "DEFINE LogarithmMacro(numbers) RETURNS logs {\n" +
            "    $logs = FOREACH $numbers GENERATE LOG($0);\n" +
            "};\n";
        createFile("nested_macros.pig", nested_macros);

        String macro =
            "%default MULTIPLIER 5\n" +
            "IMPORT 'nested_macros.pig';" +
            "DEFINE MultiplyMacro(numbers) RETURNS multiplied {\n" +
            "    $multiplied = NestedMultiplyMacro($numbers);\n" +
            "};";
        createFile("my_macros.pig", macro);

        String script =
            "IMPORT 'my_macro.pig';\n" +
            "IMPORT 'nested_macros.pig';\n" +
            "data = LOAD '1234.txt' USING PigStorage() AS (i: int);\n" +
            "mult = MultiplyMacro(data);\n" +
            "logs = LogarithmMacro(mult);\n" + 
            "STORE logs INTO 'result.out' USING PigStorage();";

        String expected =
            "data = LOAD '1234.txt' USING PigStorage() AS i:int;\n" +
            "mult = FOREACH data GENERATE $0 * 5;\n" +
            "logs = FOREACH mult GENERATE LOG($0);\n" + 
            "STORE logs INTO 'result.out' USING PigStorage();\n";

        verify(script, expected);
    }

    //-------------------------------------------------------------------------
    
    private void testMacro(String content) throws Exception {
        testMacro(content, null);
    }
    
    private void testMacro(String content, String expected) throws Exception {
        String macro = "define mymacro() returns void {" +
            content + "};";
            
        String script = macro +
            "mymacro();\n";
        
        verify(script, expected);
    }

    private void verify(String s) throws Exception {
        verify(s, null);
    }
    
    private void verify(String s, String expected) throws Exception {
        createFile("myscript.pig", s);

        String[] args = { "-Dpig.import.search.path=/tmp", "-x", "local", "-c", "myscript.pig" };
        PigStats stats = PigRunner.run(args, null);
        
        if (!stats.isSuccessful()) {
            System.out.println("error msg: " + stats.getErrorMessage());
        }
        
        assertTrue(stats.isSuccessful());
        
        String[] args2 = { "-Dpig.import.search.path=/tmp", "-x", "local", "-r", "myscript.pig" };
        PigRunner.run(args2, null);
        
        File f2 = new File("myscript.pig.expanded");
        BufferedReader br = new BufferedReader(new FileReader(f2));
        StringBuilder sb = new StringBuilder();
        String line = br.readLine();
        
        while (line != null) {
            sb.append(line).append("\n");
            line = br.readLine();
        }
        
        f2.delete();
        
        if (expected != null) {
            Assert.assertEquals(expected, sb.toString());
        } else {
            System.out.println("Result:\n" + sb.toString());
        }
    }
    
    private void validateFailure(String piglatin, String expectedErr, String keyword) throws Throwable {
        String scriptFile = "myscript.pig";
        
        ScriptState ss = ScriptState.get();
        ss.setFileName(scriptFile);
        
        try {
            BufferedReader br = new BufferedReader(new StringReader(piglatin));
            Grunt grunt = new Grunt(br, new PigContext(ExecType.LOCAL, new Properties()));
            
            PrintWriter w = new PrintWriter(new FileWriter(scriptFile));
            w.print(piglatin);
            w.close();
            
            grunt.checkScript(scriptFile);
            
            Assert.fail("Expected exception isn't thrown");
        } catch (Exception e) { 
            String msg = e.getMessage();
            int pos = msg.indexOf(keyword);
            if (pos < 0) {
                Throwable cause = e.getCause();
                if (cause != null) {
                    msg = cause.getMessage();
                    pos = msg.indexOf(keyword);
                }
            }
            Assert.assertEquals(expectedErr, msg.substring(pos, pos+expectedErr.length()));        
        } finally {
            new File(scriptFile).delete();
        }
    }
    
    private void validateDryrunFailure(String piglatin, String expectedErr,
            String keyword) throws Throwable {
        String scriptFile = "myscript.pig";

        try {
            BufferedReader br = new BufferedReader(new StringReader(piglatin));
            DryRunGruntParser parser = new DryRunGruntParser(br, scriptFile,
                    new PigContext(ExecType.LOCAL, new Properties()));

            PrintWriter w = new PrintWriter(new FileWriter(scriptFile));
            w.print(piglatin);
            w.close();

            parser.parseStopOnError();

            Assert.fail("Expected exception isn't thrown");
        } catch (Exception e) {
            String msg = e.getMessage();
            int pos = msg.indexOf(keyword);
            if (pos < 0) {
                Throwable cause = e.getCause();
                if (cause != null) {
                    msg = cause.getMessage();
                    pos = msg.indexOf(keyword);
                }
            }
            Assert.assertEquals(expectedErr,
                    msg.substring(pos, pos + expectedErr.length()));
        } finally {
            new File(scriptFile).delete();
        }
    }
    
    private void validateFailure(String piglatin, String expectedErr) throws Throwable {
        validateFailure(piglatin, expectedErr, "Reason:");
    }

    private void createFile(String filePath, String content) throws Exception {
        createFile(filePath, content, true);
    }

    private void createFile(String filePath, String content, boolean deleteOnExit) throws Exception {
        File f = new File(filePath);
        if (f.getParent() != null && !(new File(f.getParent())).exists()) {
            (new File(f.getParent())).mkdirs();
        }
        
        if (deleteOnExit) {
            f.deleteOnExit();
        }

        FileWriter fw = new FileWriter(f);
        fw.append(content);
        fw.close();
    }

    private boolean verifyImportUsingSearchPath(String macroFilePath, String importFilePath,
            String importSearchPath) throws Exception {
        return verifyImportUsingSearchPath(macroFilePath, importFilePath,
                importSearchPath, true);
    }

    private boolean verifyImportUsingSearchPath(String macroFilePath, String importFilePath,
            String importSearchPath, boolean createMacroFilePath) throws Exception {

        if (createMacroFilePath) {
            createFile(macroFilePath, groupAndCountMacro);
        }

        String script =
            "import '" + importFilePath + "';\n" +
            "alpha = load 'users' as (user, age, zip);\n" +
            "gamma = group_and_count (alpha, user, 23);\n" +
            "store gamma into 'byuser';\n";

        createFile("myscript.pig", script);

        String[] args = {
                (importSearchPath != null ? "-Dpig.import.search.path=" + importSearchPath : ""),
                "-x", "local", "-c", "myscript.pig"
        };
        PigStats stats = PigRunner.run(args, null);

        return stats.isSuccessful();
    }
    
}
