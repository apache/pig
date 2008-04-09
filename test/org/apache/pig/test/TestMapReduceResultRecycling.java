package org.apache.pig.test;

import static org.apache.pig.PigServer.ExecType ;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import junit.framework.TestCase;

public class TestMapReduceResultRecycling extends TestCase {
    MiniCluster cluster = MiniCluster.buildCluster();
    
    @Test
    public void testPlanRecycling() throws Throwable {
        PigServer pig = new PigServer(ExecType.MAPREDUCE);
        File tmpFile = this.createTempFile();
        {            
            String query = "a = load 'file:" + tmpFile + "'; " ;
            System.out.println(query);
            pig.registerQuery(query);
            pig.explain("a", System.out) ;
            Iterator<Tuple> it = pig.openIterator("a");
            assertTrue(it.next().getAtomField(0).strval().equals("a1")) ;
            assertTrue(it.next().getAtomField(0).strval().equals("b1")) ;
            assertTrue(it.next().getAtomField(0).strval().equals("c1")) ;
            assertFalse(it.hasNext()) ;
        }
       
        {
            String query = "b = filter a by $0 eq 'a1';" ;
            System.out.println(query);
            pig.registerQuery(query);
            pig.explain("b", System.out) ;
            Iterator<Tuple> it = pig.openIterator("b");
            assertTrue(it.next().getAtomField(0).strval().equals("a1")) ;
            assertFalse(it.hasNext()) ;
        }
        
        {
            String query = "c = filter a by $0 eq 'b1';" ;
            System.out.println(query);
            pig.registerQuery(query);
            pig.explain("c", System.out) ;
            Iterator<Tuple> it = pig.openIterator("c");
            assertTrue(it.next().getAtomField(0).strval().equals("b1")) ;
            assertFalse(it.hasNext()) ;
        }
        
    }
    
    private File createTempFile() throws Throwable {
        File tmpFile = File.createTempFile("pi_test1", "txt");
        tmpFile.deleteOnExit() ;
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        ps.println("a1\t1\t1000") ;
        ps.println("b1\t2\t1000") ;
        ps.println("c1\t3\t1000") ;
        ps.close(); 
        return tmpFile ;
    }
}
