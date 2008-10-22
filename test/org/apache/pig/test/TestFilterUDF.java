package org.apache.pig.test;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFilterUDF extends TestCase {
    private PigServer pigServer;
    private MiniCluster cluster = MiniCluster.buildCluster();
    private File tmpFile;
    
    public TestFilterUDF() throws ExecException, IOException{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
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
    
    static public class MyFilterFunction extends EvalFunc<Boolean>{

        @Override
        public Boolean exec(Tuple input) throws IOException {
            try {
                int col = (Integer)input.get(0);
                if(col>10)
                    return true;
            } catch (ExecException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return false;
        }
        
    }
    
    @Test
    public void testFilterUDF() throws Exception{
        
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x:int);");
        pigServer.registerQuery("B = filter A by " + MyFilterFunction.class.getName() + "();");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals(true,(Integer)t.get(0)>10);
            ++cnt;
        }
        assertEquals(10, cnt);
    }
}
