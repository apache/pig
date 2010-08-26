package org.apache.pig.piggybank.test.storage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.Util;
import org.junit.Test;

public class TestCSVStorage {
    protected static final Log LOG = LogFactory.getLog(TestCSVStorage.class);
    
    private PigServer pigServer;
    private MiniCluster cluster;
    
    public TestCSVStorage() throws ExecException, IOException {
        cluster = MiniCluster.buildCluster();
        pigServer = new PigServer(ExecType.LOCAL, cluster.getProperties());
        pigServer.getPigContext().getProperties()
                .setProperty("mapred.map.max.attempts", "1");
        pigServer.getPigContext().getProperties()
                .setProperty("mapred.reduce.max.attempts", "1");
    }

    @Test
    public void testSimpleCsv() throws IOException {
        String inputFileName = "TestCSVLoader-simple.txt";
        Util.createLocalInputFile(inputFileName, new String[] {"foo,bar,baz", "fee,foe,fum"});
        String script = "a = load '" + inputFileName + "' using org.apache.pig.piggybank.storage.CSVLoader() " +
        "   as (a:chararray, b:chararray, c:chararray); ";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("a");
        assertEquals(Util.createTuple(new String[] {"foo", "bar", "baz"}), it.next());
    }
   
    @Test 
    public void testQuotedCommas() throws IOException {
        String inputFileName = "TestCSVLoader-quotedcommas.txt";
        Util.createLocalInputFile(inputFileName, new String[] {"\"foo,bar,baz\"", "fee,foe,fum"});
        String script = "a = load '" + inputFileName + "' using org.apache.pig.piggybank.storage.CSVLoader() " +
        "   as (a:chararray, b:chararray, c:chararray); ";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("a");
        assertEquals(Util.createTuple(new String[] {"foo,bar,baz", null, null}), it.next());
        assertEquals(Util.createTuple(new String[] {"fee", "foe", "fum"}), it.next());
    }
    
    @Test
    public void testQuotedQuotes() throws IOException {
        String inputFileName = "TestCSVLoader-quotedquotes.txt";
        Util.createLocalInputFile(inputFileName, 
                new String[] {"\"foo,\"\"bar\"\",baz\"", "\"\"\"\"\"\"\"\""});
        String script = "a = load '" + inputFileName + "' using org.apache.pig.piggybank.storage.CSVLoader() " +
        "   as (a:chararray); ";
        Util.registerMultiLineQuery(pigServer, script);
        Iterator<Tuple> it = pigServer.openIterator("a");
        assertEquals(Util.createTuple(new String[] {"foo,\"bar\",baz"}), it.next());
        assertEquals(Util.createTuple(new String[] {"\"\"\""}), it.next());
    }
    
}
