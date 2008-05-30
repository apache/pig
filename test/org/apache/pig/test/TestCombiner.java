package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

public class TestCombiner extends PigExecTestCase {

    public void testCombiner() throws IOException {
        List<String> inputLines = new ArrayList<String>();
        inputLines.add("a,b,1");
        inputLines.add("a,b,1");
        inputLines.add("a,c,1");
        loadWithTestLoadFunc("A", pigServer, inputLines);

        pigServer.registerQuery("B = group A by ($0, $1);");
        pigServer.registerQuery("C = foreach B generate flatten(group), COUNT($1);");
        Iterator<Tuple> resultIterator = pigServer.openIterator("C");
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
                + Util.encodeEscape(inputFile.toString()) + "' using "
                + PigStorage.class.getName() + "(',');");
    }

}
