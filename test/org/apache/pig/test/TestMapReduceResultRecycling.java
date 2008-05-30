package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestMapReduceResultRecycling extends PigExecTestCase {
    
    @Test
    public void testPlanRecycling() throws Throwable {
        File tmpFile = this.createTempFile();
        {            
            String query = "a = load 'file:" + Util.encodeEscape(tmpFile.toString()) + "'; " ;
            System.out.println(query);
            pigServer.registerQuery(query);
            pigServer.explain("a", System.out) ;
            Iterator<Tuple> it = pigServer.openIterator("a");
            assertTrue(it.next().getAtomField(0).strval().equals("a1")) ;
            assertTrue(it.next().getAtomField(0).strval().equals("b1")) ;
            assertTrue(it.next().getAtomField(0).strval().equals("c1")) ;
            assertFalse(it.hasNext()) ;
        }
       
        {
            String query = "b = filter a by $0 eq 'a1';" ;
            System.out.println(query);
            pigServer.registerQuery(query);
            pigServer.explain("b", System.out) ;
            Iterator<Tuple> it = pigServer.openIterator("b");
            assertTrue(it.next().getAtomField(0).strval().equals("a1")) ;
            assertFalse(it.hasNext()) ;
        }
        
        {
            String query = "c = filter a by $0 eq 'b1';" ;
            System.out.println(query);
            pigServer.registerQuery(query);
            pigServer.explain("c", System.out) ;
            Iterator<Tuple> it = pigServer.openIterator("c");
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
