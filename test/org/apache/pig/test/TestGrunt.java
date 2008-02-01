package org.apache.pig.test;

import org.junit.Test;
import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.Grunt;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class TestGrunt extends TestCase {

	
	@Test 
	public void testCopyFromLocal() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "copyFromLocal /tmp/TestMe;";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
	
        grunt.exec();
	}
}
