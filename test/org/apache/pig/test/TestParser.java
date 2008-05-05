package org.apache.pig.test;

import java.io.IOException;

import org.apache.pig.PigServer.ExecType;
import org.apache.pig.backend.executionengine.ExecException;

public class TestParser extends PigExecTestCase {

    public void testLoadingNonexistentFile() throws ExecException, IOException {
        try {
            // FIXME : this should be tested in all modes
            if(execType == ExecType.LOCAL)
                return;
            pigServer.registerQuery("vals = load 'nonexistentfile';");
            fail("Loading a  nonexistent file should throw an IOException at parse time");
        } catch (IOException io) {
        }
    }
}
