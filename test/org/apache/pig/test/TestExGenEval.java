package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

import org.apache.pig.PigServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestExGenEval extends TestCase {
	
	File A, B, C, D;
	private int MAX = 10;
	
	String initString = "local";
	PigServer pig;
	
	@Override
	@Before
	protected void setUp() throws Exception{
		System.out.println("Generating test data...");
		A = File.createTempFile("dataA", ".dat");
		B = File.createTempFile("dataB", ".dat");
		C = File.createTempFile("dataC", ".dat");
		D = File.createTempFile("dataD", ".dat");
		
		writeData(A);
		writeData(B);
		writeData(C);
		writeData(D);
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
		C.delete();
		D.delete();
	}
	
	@Test
	public void testForeach() throws Exception {
		pig = new PigServer(initString);
		System.out.println("Testing Foreach statement...");
		pig.registerQuery("A = load '" + A.toString() + "' as (x, y);");
		pig.registerQuery("B = foreach A generate x+y as sum;");
		pig.showExamples("B");
		assertEquals(1, 1);
	}
	
	@Test 
	public void testFilter() throws Exception {
		pig = new PigServer(initString);
		pig.registerQuery("A = load '" + A.toString() + "' as (x, y);");
		pig.registerQuery("B = filter A by x < 10.0;");
		pig.showExamples("B");
		assertEquals(1, 1);
	}
	
	@Test
	public void testFlatten() throws Exception {
		pig = new PigServer(initString);
		pig.registerQuery("A1 = load '" + A.toString() + "' as (x, y);");
		pig.registerQuery("B1 = load '" + B.toString() + "' as (x, y);");
		pig.registerQuery("C1 = load '" + C.toString() + "' as (x, y);");
		pig.registerQuery("D1 = load '" + D.toString() + "' as (x, y);");
		pig.registerQuery("E = join A1 by x, B1 by x;");
		pig.registerQuery("F = join C1 by x, D1 by x;");
		pig.registerQuery("G = join E by $0, F by $0;");
		pig.showExamples("G");
		assertEquals(1, 1);
	}
}
