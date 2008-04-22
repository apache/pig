package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

import org.apache.pig.PigServer;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestExGenCogroup extends TestCase{
	String A, B;
	private int MAX = 10;
	
	String initString = "mapreduce";
	PigServer pig;
	
	MiniCluster cluster = MiniCluster.buildCluster();
	
	@Override
	@Before
	protected void setUp() throws Exception{
		File fileA, fileB;
		pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
		System.out.println("Generating test data...");
		fileA = File.createTempFile("dataA", ".dat");
		fileB = File.createTempFile("dataB", ".dat");
		
		writeData(fileA);
		writeData(fileB);
		
		A = "'" + FileLocalizer.hadoopify(fileA.toString(), pig.getPigContext()) + "'";
		B = "'" + FileLocalizer.hadoopify(fileB.toString(), pig.getPigContext()) + "'";
		System.out.println("A : " + A  + "\n" + "B : " + B);
		System.out.println("Test data created.");
		
	}
	
	private void writeData(File dataFile) throws Exception{
		//File dataFile = File.createTempFile(name, ".dat");
		FileOutputStream dat = new FileOutputStream(dataFile);
		
		Random rand = new Random();
		
		for(int i = 0; i < MAX; i++) 
			dat.write((rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n").getBytes());
		dat.close();
			
	}
	
	@Override
	@After
	protected void tearDown() throws Exception {
		
	
	}
	
	@Test
	public void testCogroupMultipleCols() throws Exception {
		//pig = new PigServer(initString);
		pig.registerQuery("A = load " + A + " as (x, y);");
		pig.registerQuery("B = load " + B + " as (x, y);");
		pig.registerQuery("C = cogroup A by (x, y), B by (x, y);");
		pig.showExamples("C");
	}
	
	@Test
	public void testCogroup() throws Exception {
		//pig = new PigServer(initString);
		pig.registerQuery("A = load " + A + " as (x, y);");
		pig.registerQuery("B = load " + B + " as (x, y);");
		pig.registerQuery("C = cogroup A by x, B by x;");
		pig.showExamples("C");
	}
	
	@Test
	public void testGroup() throws Exception {
		//pig = new PigServer(initString);
		pig.registerQuery("A = load " + A.toString() + " as (x, y);");
		pig.registerQuery("B = group A by x;");
		pig.showExamples("B");
		
	}
	
	@Test
	public void testComplexGroup() throws Exception {
		//pig = new PigServer(initString);
		pig.registerQuery("A = load " + A.toString() + " as (x, y);");
		pig.registerQuery("B = load " + B.toString() + " as (x, y);");
		pig.registerQuery("C = cogroup A by x, B by x;");
		pig.registerQuery("D = cogroup A by y, B by y;");
		pig.registerQuery("E = cogroup C by $0, D by $0;");
		pig.showExamples("E");
	}
	
}
