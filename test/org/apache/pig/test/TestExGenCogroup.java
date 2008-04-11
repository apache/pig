package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

import org.apache.pig.PigServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestExGenCogroup extends TestCase{
	File A, B;
	private int MAX = 10;
	
	String initString = "local";
	PigServer pig;
	
	@Override
	@Before
	protected void setUp() throws Exception{
		System.out.println("Generating test data...");
		A = File.createTempFile("dataA", ".dat");
		B = File.createTempFile("dataB", ".dat");
		
		writeData(A);
		writeData(B);
		System.out.println("Test data created.");
		
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
		A.delete();
		B.delete();
	
	}
	
	@Test
	public void testCogroupMultipleCols() throws Exception {
		pig = new PigServer(initString);
		pig.registerQuery("A = load '" + A.toString() + "' as (x, y);");
		pig.registerQuery("B = load '" + B.toString() + "' as (x, y);");
		pig.registerQuery("C = cogroup A by (x, y), B by (x, y);");
		pig.showExamples("C");
	}
	
	@Test
	public void testCogroup() throws Exception {
		pig = new PigServer(initString);
		pig.registerQuery("A = load '" + A.toString() + "' as (x, y);");
		pig.registerQuery("B = load '" + B.toString() + "' as (x, y);");
		pig.registerQuery("C = cogroup A by x, B by x;");
		pig.showExamples("C");
	}
	
	@Test
	public void testGroup() throws Exception {
		pig = new PigServer(initString);
		pig.registerQuery("A = load '" + A.toString() + "' as (x, y);");
		pig.registerQuery("B = group A by x;");
		pig.showExamples("B");
		
	}
	
	@Test
	public void testComplexGroup() throws Exception {
		pig = new PigServer(initString);
		pig.registerQuery("A = load '" + A.toString() + "' as (x, y);");
		pig.registerQuery("B = load '" + B.toString() + "' as (x, y);");
		pig.registerQuery("C = cogroup A by x, B by x;");
		pig.registerQuery("D = cogroup A by y, B by y;");
		pig.registerQuery("E = cogroup C by $0, D by $0;");
		pig.showExamples("E");
	}
	
}
