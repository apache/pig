package org.apache.pig.test;


import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestImplicitSplit extends TestCase{
    private PigServer pigServer;
    
    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testImplicitSplit() throws Exception{
        int LOOP_SIZE = 20;
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 1; i <= LOOP_SIZE; i++) {
            ps.println(i);
        }
        ps.close();
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = filter A by $0<=10;");
        pigServer.registerQuery("C = filter A by $0>10;");
        pigServer.registerQuery("D = union B,C;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            ++cnt;
        }
        assertEquals(20, cnt);
    }
}
