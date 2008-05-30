/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.test;

import static org.apache.pig.PigServer.ExecType.MAPREDUCE;
import org.apache.pig.PigServer;

import org.apache.pig.data.Tuple;

import org.apache.pig.impl.io.FileLocalizer;

import org.apache.pig.backend.executionengine.ExecException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
/*
 * Testcase aimed at testing pig with large file sizes and filter and group functions
*/
public class TestPi extends TestCase {
	
    private final Log log = LogFactory.getLog(getClass());

	File datFile;
	private long defaultBlockSize = (new Configuration()).getLong("dfs.block.size", 0);
	
	private long total = ((defaultBlockSize >> 20) / 10) << 20;
	private int inCircle = 0;
	MiniCluster cluster = MiniCluster.buildCluster();

	private long totalLength = 0, totalLengthTest = 0;

	
	
	PigServer pig;
	String fileName, tmpFile1;
	
	@Override
	@Before
    protected void setUp() throws Exception{

        log.info("Generating test data...");
        log.info("Default block size = " + defaultBlockSize);
        log.info("Total no. of iterations to run for test data = " + total);
        datFile = File.createTempFile("PiTest", ".dat");
        
        FileOutputStream dat = new FileOutputStream(datFile);
        
        Random rand = new Random();
        
        for(int i = 0; i < total; i++) {
            StringBuilder sb = new StringBuilder();
            
            Double x = new Double(rand.nextDouble());
            Double y = new Double(rand.nextDouble());
            double x1 = x.doubleValue() - 0.5;
            double y1 = y.doubleValue() - 0.5;
            double sq_dist = (x1*x1) + (y1*y1); 
            if(sq_dist <= 0.25) {
            	inCircle ++;

            }

            totalLength += String.valueOf(sq_dist).length();

            sb.append(x.toString() + ":" + y.toString() + "\n");
            
            dat.write(sb.toString().getBytes());
                        
        }
        
        dat.close();
        
        try {
            pig = new PigServer(MAPREDUCE, cluster.getProperties());
        }
        catch (ExecException e) {
        	IOException ioe = new IOException("Failed to create Pig Server");
        	ioe.initCause(e);
            throw ioe;
        }
        
        fileName = "'" + FileLocalizer.hadoopify(datFile.toString(), pig.getPigContext()) + "'";
		tmpFile1 = "'" + FileLocalizer.getTemporaryPath(null, pig.getPigContext()).toString() + "'";

        datFile.delete();
    }
	
	@Override
	@After
	protected void tearDown() throws Exception {
		
	}
	
	@Test
	public void testPi () throws Exception {
		
		pig.registerQuery("A = load " + Util.encodeEscape(fileName.toString()) + " using PigStorage(':');");
		pig.registerQuery("B = foreach A generate $0 - '0.5' as d1, $1 - '0.5' as d2;");
		pig.registerQuery("C = foreach B generate $0 * $0 as m1, $1 * $1 as m2;");
		pig.registerQuery("D = foreach C generate $0 + $1 as s1;");
		pig.registerQuery("D = foreach D generate $0, ARITY($0);");
		pig.store("D", Util.encodeEscape(tmpFile1.toString()));

		pig.registerQuery("E = filter D by $0 <= '0.25';");

		pig.registerQuery("F = group D by $1;");
		pig.registerQuery("G = group E by $1;");

		pig.registerQuery("J = foreach F generate COUNT($1);");
		pig.registerQuery("K = foreach G generate COUNT($1);");
		

		Iterator <Tuple> Total = pig.openIterator("J");
		Iterator <Tuple> InCircle = pig.openIterator("K");

		
		int totalPoints = Total.next().getAtomField(0).numval().intValue();
		int inCirclePoints = InCircle.next().getAtomField(0).numval().intValue();

		log.info("Value of PI = " + 4 * (double)inCircle / (double)total);
		log.info("Value of PI (From Test data) = " + 4 * (double)inCirclePoints / (double)totalPoints);
		
		
		Iterator <Tuple> lengthTest = pig.openIterator("D");
		
		while(lengthTest.hasNext()) {
			Tuple temp = lengthTest.next();
			totalLengthTest += temp.getAtomField(0).toString().length();
		}
		
		assertEquals(totalPoints, total);
		assertEquals(inCirclePoints, inCircle);
		assertEquals(totalLengthTest, totalLength);

		

			
	}

}
