package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import org.apache.pig.PigServer;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestExGenEval extends TestCase {
	
	String A, B, C, D;
	private int MAX = 10;
	
	String initString = "mapreduce";
	PigServer pig;
	PigContext pigContext;
	
	MiniCluster cluster = MiniCluster.buildCluster();
	
	@Override
	@Before
	protected void setUp() throws Exception{
		System.out.println("Generating test data...");
		File fileA, fileB, fileC, fileD;
		
		pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
		pigContext = pig.getPigContext();
		fileA = File.createTempFile("dataA", ".dat");
		fileB = File.createTempFile("dataB", ".dat");
		fileC = File.createTempFile("dataC", ".dat");
		fileD = File.createTempFile("dataD", ".dat");
		
		
		writeData(fileA);
		writeData(fileB);
		writeData(fileC);
		writeData(fileD);
		
		
		A = "'" + FileLocalizer.hadoopify(fileA.toString(), pig.getPigContext()) + "'";
		B = "'" + FileLocalizer.hadoopify(fileB.toString(), pig.getPigContext()) + "'";
		C = "'" + FileLocalizer.hadoopify(fileC.toString(), pig.getPigContext()) + "'";
		D = "'" + FileLocalizer.hadoopify(fileD.toString(), pig.getPigContext()) + "'";
		
		System.out.println("Test data created.");
		fileA.delete();
		fileB.delete();
		fileC.delete();
		fileD.delete();
		
	}
	
	private void writeData(File dataFile) throws Exception{
		//File dataFile = File.createTempFile(name, ".dat");
		FileOutputStream dat = new FileOutputStream(dataFile);
		
		Random rand = new Random();
		
		for(int i = 0; i < MAX; i++) 
			dat.write((rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n").getBytes());
			
	}
	
	@Override
	@After
	protected void tearDown() throws Exception {
		
	}
	
	@Test
	public void testForeach() throws Exception {
		//pig = new PigServer(initString);
		System.out.println("Testing Foreach statement...");
		pig.registerQuery("A = load " + A + " as (x, y);");
		pig.registerQuery("B = foreach A generate x+y as sum;");
		pig.showExamples("B");
		assertEquals(1, 1);
	}
	
	@Test 
	public void testFilter() throws Exception {
		//pig = new PigServer(initString);
		pig.registerQuery("A = load " + A + " as (x, y);");
		pig.registerQuery("B = filter A by x < 10.0;");
		pig.showExamples("B");
		assertEquals(1, 1);
	}
	
	@Test
	public void testFlatten() throws Exception {
		//pig = new PigServer(initString);
		pig.registerQuery("A1 = load " + A + " as (x, y);");
		pig.registerQuery("B1 = load " + B + " as (x, y);");
		pig.registerQuery("C1 = load " + C + " as (x, y);");
		pig.registerQuery("D1 = load " + D + " as (x, y);");
		pig.registerQuery("E = join A1 by x, B1 by x;");
		pig.registerQuery("F = join C1 by x, D1 by x;");
		pig.registerQuery("G = join E by $0, F by $0;");
		pig.showExamples("G");
		assertEquals(1, 1);
	}
}
