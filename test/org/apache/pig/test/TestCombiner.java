package org.apache.pig.test;

import static org.apache.pig.PigServer.ExecType.MAPREDUCE;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

public class TestCombiner extends TestCase {

    @Test
    public void testLocal() throws Exception {
        // run the test locally
        runTest(new PigServer("local"));
    }

    @Test
    public void testOnCluster() throws Exception {
        // run the test on cluster
        MiniCluster.buildCluster();
        runTest(new PigServer(MAPREDUCE));

    }

    private void runTest(PigServer pig) throws IOException {
        List<String> inputLines = new ArrayList<String>();
        inputLines.add("a,b,1");
        inputLines.add("a,b,1");
        inputLines.add("a,c,1");
        loadWithTestLoadFunc("A", pig, inputLines);

        pig.registerQuery("B = group A by ($0, $1);");
        pig.registerQuery("C = foreach B generate flatten(group), COUNT($1);");
        Iterator<Tuple> resultIterator = pig.openIterator("C");
        Tuple tuple = resultIterator.next();
        assertEquals("(a, b, 2)", tuple.toString());
        tuple = resultIterator.next();
        assertEquals("(a, c, 1)", tuple.toString());
    }

    private void loadWithTestLoadFunc(String loadAlias, PigServer pig,
            List<String> inputLines) throws IOException {
        File inputFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(inputFile));
        for (String line : inputLines) {
            ps.println(line);
        }
        ps.close();
        pig.registerQuery(loadAlias + " = load 'file:"
                + inputFile + "' using "
                + PigStorage.class.getName() + "(',');");
    }

}
