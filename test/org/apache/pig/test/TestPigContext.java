package org.apache.pig.test;

import static org.apache.pig.PigServer.ExecType.LOCAL;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPigContext extends TestCase {

    private static final String TMP_DIR_PROP = "/tmp/hadoop-hadoop";
    private static final String FS_NAME = "machine:9000";
    private static final String JOB_TRACKER = "machine:9001";

    private File input;
    private PigContext pigContext;
    
    @Before
    @Override
    protected void setUp() throws Exception {
        pigContext = new PigContext(LOCAL, getProperties());
        input = File.createTempFile("PigContextTest-", ".txt");
    }
    
    /**
     * Passing an already configured pigContext in PigServer constructor. 
     */
    @Test
    public void testSetProperties_way_num01() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        registerAndStore(pigServer);
        
        check_asserts();
    }

    /**
     * Setting properties through PigServer constructor directly. 
     */
    @Test
    public void testSetProperties_way_num02() throws Exception {
        PigServer pigServer = new PigServer(LOCAL, getProperties());
        registerAndStore(pigServer);
        
        check_asserts();
    }

    /**
     * using connect() method. 
     */
    @Test
    public void testSetProperties_way_num03() throws Exception {
        pigContext.connect();
        PigServer pigServer = new PigServer(pigContext);
        registerAndStore(pigServer);
        
        check_asserts();
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        input.delete();
    }
    
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("mapred.job.tracker", JOB_TRACKER);
        props.put("fs.default.name", FS_NAME);
        props.put("hadoop.tmp.dir", TMP_DIR_PROP);
        return props;
    }

    private List<String> getCommands() {
        List<String> commands = new ArrayList<String>();
        commands.add("my_input = LOAD '" + Util.encodeEscape(input.getAbsolutePath().toString()) + "' USING PigStorage();");
        commands.add("words = FOREACH my_input GENERATE FLATTEN(TOKENIZE(*));");
        commands.add("grouped = GROUP words BY $0;");
        commands.add("counts = FOREACH grouped GENERATE group, COUNT(words);");
        return commands;
    }

    private void registerAndStore(PigServer pigServer) throws IOException {
        pigServer.debugOn();
        List<String> commands = getCommands();
        for (final String command : commands) {
            pigServer.registerQuery(command);
        }
        pigServer.store("counts", input.getAbsolutePath() + ".out");
    }

    private void check_asserts() {
        assertEquals(JOB_TRACKER, pigContext.getProperties().getProperty("mapred.job.tracker"));
        assertEquals(FS_NAME, pigContext.getProperties().getProperty("fs.default.name"));
        assertEquals(TMP_DIR_PROP, pigContext.getProperties().getProperty("hadoop.tmp.dir"));
    }
}
