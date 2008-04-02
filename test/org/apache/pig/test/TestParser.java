package org.apache.pig.test;

import static org.apache.pig.PigServer.ExecType.MAPREDUCE;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class TestParser extends TestCase {

    public void testLoadingNonexistentFile() throws ExecException, IOException {
        PigServer pig = new PigServer(MAPREDUCE);
        try {
            pig.registerQuery("vals = load 'nonexistentfile';");
            fail("Loading a  nonexistent file should throw an IOException at parse time");
        } catch (IOException io) {
        }
    }
}
