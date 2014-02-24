package org.apache.pig.test;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestAutoLocalMode {

    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;
    private File logFile;

    private String miniFileName;
    private String smallFileName;
    private String bigFileName;

    private File createInputFile(String filename, long minSizeInBytes) throws IOException {
        File tmpFile = Util.createTempFileDelOnExit(filename, "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random(1);
        int rand;
        while (tmpFile.length() < minSizeInBytes) {
            rand = r.nextInt(100);
            ps.println(rand);
        }
        ps.close();
        return tmpFile;
    }

    @SuppressWarnings("resource")
    private boolean checkLogFileMessage(String[] messages) {
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(logFile));
            List<String> logMessages=new ArrayList<String>();
            String line;
            while ((line=reader.readLine())!=null)
            {
                logMessages.add(line);
            }
            // Messages should appear in the expected order
            int i = 0;
            for(String logMsg : logMessages) {
                if (logMsg.contains(messages[i])) {
                    i++;
                    if(i == messages.length) {
                        return true;
                    }
                }
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    @Before
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.getPigContext().getExecutionEngine().setProperty(PigConfiguration.OPT_FETCH, "false");
        pigServer.getPigContext().getExecutionEngine().setProperty(PigConfiguration.PIG_AUTO_LOCAL_ENABLED, String.valueOf("true"));
        pigServer.getPigContext().getExecutionEngine().setProperty(PigConfiguration.PIG_AUTO_LOCAL_INPUT_MAXBYTES, "200");

        Logger logger = Logger.getLogger(JobControlCompiler.class);
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);
        SimpleLayout layout = new SimpleLayout();
        logFile = File.createTempFile("log", "");
        FileAppender appender = new FileAppender(layout, logFile.toString(), false, false, 0);
        logger.addAppender(appender);

        miniFileName = createInputFile("miniFile", 10).getAbsolutePath();
        smallFileName = createInputFile("smallFile", 100).getAbsolutePath();
        bigFileName = createInputFile("bigFile", 1000).getAbsolutePath();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testSmallJob() throws IOException {
        pigServer.registerQuery("A = LOAD '"
                + Util.generateURI(Util.encodeEscape(smallFileName), pigServer
                        .getPigContext()) + "' AS (num:int);");
        pigServer.registerQuery("B = filter A by 1 == 0;");
        pigServer.openIterator("B");

        assertTrue(checkLogFileMessage(new String[]{JobControlCompiler.SMALL_JOB_LOG_MSG}));
    }

    @Test
    public void testBigJob() throws IOException {
        pigServer.registerQuery("A = LOAD '"
                + Util.generateURI(Util.encodeEscape(bigFileName), pigServer
                        .getPigContext()) + "' AS (num:int);");
        pigServer.registerQuery("B = filter A by 1 == 0;");
        pigServer.openIterator("B");

        assertTrue(checkLogFileMessage(new String[]{JobControlCompiler.BIG_JOB_LOG_MSG}));
    }

    @Test
    public void testReplicatedJoin() throws IOException {
        pigServer.registerQuery("A1 = LOAD '"
                + Util.generateURI(Util.encodeEscape(smallFileName), pigServer
                        .getPigContext()) + "' AS (num:int);");
        pigServer.registerQuery("A2 = LOAD '"
                + Util.generateURI(Util.encodeEscape(miniFileName), pigServer
                        .getPigContext()) + "' AS (num:int);");
        pigServer.registerQuery("A = join A1 by num, A2 by num using 'replicated';");
        pigServer.registerQuery("B = filter A by 1 == 0;");
        pigServer.openIterator("B");

        assertTrue(checkLogFileMessage(new String[]{JobControlCompiler.SMALL_JOB_LOG_MSG}));
    }

    @Test
    public void testOrderBy() throws IOException {
        pigServer.registerQuery("A1 = LOAD '"
                + Util.generateURI(Util.encodeEscape(bigFileName), pigServer
                        .getPigContext()) + "' AS (num:int);");
        pigServer.registerQuery("A = filter A1 by num == 1;");
        pigServer.registerQuery("B = order A by num;");
        pigServer.openIterator("B");

        assertTrue(checkLogFileMessage(new String[]{
                JobControlCompiler.BIG_JOB_LOG_MSG,
                JobControlCompiler.SMALL_JOB_LOG_MSG,
                JobControlCompiler.SMALL_JOB_LOG_MSG}));
    }


}
