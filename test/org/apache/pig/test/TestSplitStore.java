package org.apache.pig.test;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestSplitStore extends TestCase{
    private PigServer pig;
    private PigContext pigContext;
    private File tmpFile;
    private MiniCluster cluster = MiniCluster.buildCluster();
    
    public TestSplitStore() throws ExecException, IOException{
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigContext = pig.getPigContext();
        int LOOP_SIZE = 20;
        tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 1; i <= LOOP_SIZE; i++) {
            ps.println(i);
        }
        ps.close();
    }
    @Before
    public void setUp() throws Exception {
        
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void test1() throws Exception{
        pig.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.store("A1", "'" + FileLocalizer.getTemporaryPath(null, pigContext) + "'");
        pig.store("A2", "'" + FileLocalizer.getTemporaryPath(null, pigContext) + "'");
    }
    
    @Test
    public void test2() throws Exception{
        pig.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.openIterator("A1");
        pig.store("A2", "'" + FileLocalizer.getTemporaryPath(null, pigContext) + "'");
    }
    
    @Test
    public void test3() throws Exception{
        pig.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.openIterator("A2");
        pig.store("A1", "'" + FileLocalizer.getTemporaryPath(null, pigContext) + "'");
    }
    
    @Test
    public void test4() throws Exception{
        pig.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.store("A1", "'" + FileLocalizer.getTemporaryPath(null, pigContext) + "'");
        pig.openIterator("A2");
    }
    
    @Test
    public void test5() throws Exception{
        pig.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.store("A2", "'" + FileLocalizer.getTemporaryPath(null, pigContext) + "'");
        pig.openIterator("A1");
    }
    
    @Test
    public void test6() throws Exception{
        pig.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.openIterator("A1");
        pig.registerQuery("Store A2 into '" + FileLocalizer.getTemporaryPath(null, pigContext) + "';");
    }
    
    @Test
    public void test7() throws Exception{
        pig.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.openIterator("A2");
        pig.registerQuery("Store A1 into '" + FileLocalizer.getTemporaryPath(null, pigContext) + "';");
    }
    
    @Test
    public void test8() throws Exception{
        pig.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.registerQuery("Store A1 into '" + FileLocalizer.getTemporaryPath(null, pigContext) + "';");
        pig.openIterator("A2");
    }
    
    @Test
    public void test9() throws Exception{
        pig.registerQuery("A = LOAD 'file:" + tmpFile + "';");
        pig.registerQuery("Split A into A1 if $0<=10, A2 if $0>10;");
        pig.registerQuery("Store A2 into '" + FileLocalizer.getTemporaryPath(null, pigContext) + "';");
        pig.openIterator("A1");
    }
}
