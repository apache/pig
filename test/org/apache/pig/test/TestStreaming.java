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

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.pig.PigServer;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.*;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;

import static org.apache.pig.PigServer.ExecType.MAPREDUCE;

import junit.framework.TestCase;

public class TestStreaming extends TestCase {

    MiniCluster cluster = MiniCluster.buildCluster();

	private static final String simpleEchoStreamingCommand = 
		"perl -ne 'chomp $_; print \"$_\n\"'";

    private Tuple[] setupExpectedResults(String[] firstField, int[] secondField) {
		Assert.assertEquals(firstField.length, secondField.length);
		
		Tuple[] expectedResults = new Tuple[firstField.length];
		for (int i=0; i < expectedResults.length; ++i) {
			expectedResults[i] = new Tuple(2);
			expectedResults[i].setField(0, firstField[i]);
			expectedResults[i].setField(1, secondField[i]);
		}
		
		return expectedResults;
	}
	
	@Test
	public void testLocalSimpleMapSideStreaming() throws Exception {
	    testSimpleMapSideStreaming(ExecType.LOCAL);
	}
	
	@Test
    public void testMRSimpleMapSideStreaming() throws Exception {
        testSimpleMapSideStreaming(ExecType.MAPREDUCE);
    }
    
	private void testSimpleMapSideStreaming(ExecType execType) 
	throws Exception {
	        PigServer pigServer = createPigServer(execType);
		File input = Util.createInputFile("tmp", "", 
				                          new String[] {"A,1", "B,2", "C,3", "D,2",
				                                        "A,5", "B,5", "C,8", "A,8",
				                                        "D,8", "A,9"});

		// Expected results
		String[] expectedFirstFields = new String[] {"A", "B", "C", "A", "D", "A"};
		int[] expectedSecondFields = new int[] {5, 5, 8, 8, 8, 9};
		Tuple[] expectedResults = 
			setupExpectedResults(expectedFirstFields, expectedSecondFields);

		// Pig query to run
		pigServer.registerQuery("IP = load 'file:" + input + "' using " + 
				                PigStorage.class.getName() + "(',');");
		pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("S1 = stream FILTERED_DATA through `" +
                                simpleEchoStreamingCommand + "`;");
		pigServer.registerQuery("OP = stream S1 through `" +
				                simpleEchoStreamingCommand + "`;");
		
		// Run the query and check the results
		Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
	}

    private PigServer createPigServer(ExecType execType) throws ExecException {
        PigServer pigServer; 
        if (execType == ExecType.MAPREDUCE) {
            pigServer = new PigServer(execType, cluster.getProperties());
        } else {
            pigServer = new PigServer(execType);
        }
        return pigServer;
    }

	@Test
    public void testLocalSimpleMapSideStreamingWithOutputSchema() 
	throws Exception {
	    testSimpleMapSideStreamingWithOutputSchema(ExecType.LOCAL);
	}
	
    @Test
    public void testMRSimpleMapSideStreamingWithOutputSchema() 
    throws Exception {
        testSimpleMapSideStreamingWithOutputSchema(ExecType.MAPREDUCE);
    }
    
	private void testSimpleMapSideStreamingWithOutputSchema(ExecType execType) 
	throws Exception {
		PigServer pigServer = createPigServer(execType);

		File input = Util.createInputFile("tmp", "", 
				                          new String[] {"A,1", "B,2", "C,3", "D,2",
				                                        "A,5", "B,5", "C,8", "A,8",
				                                        "D,8", "A,9"});

		// Expected results
		String[] expectedFirstFields = new String[] {"C", "A", "D", "A"};
		int[] expectedSecondFields = new int[] {8, 8, 8, 9};
		Tuple[] expectedResults = 
			setupExpectedResults(expectedFirstFields, expectedSecondFields);

		// Pig query to run
		pigServer.registerQuery("IP = load 'file:" + input + "' using " + 
				                PigStorage.class.getName() + "(',');");
		pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
		pigServer.registerQuery("STREAMED_DATA = stream FILTERED_DATA through `" +
				                simpleEchoStreamingCommand + "` as (f0, f1);");
		pigServer.registerQuery("OP = filter STREAMED_DATA by f1 > '6';");
		
		// Run the query and check the results
		Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
	}

	@Test
    public void testLocalSimpleReduceSideStreamingAfterFlatten() 
	throws Exception {
	    testSimpleReduceSideStreamingAfterFlatten(ExecType.LOCAL);
	}
	
    @Test
    public void testMRSimpleReduceSideStreamingAfterFlatten() 
    throws Exception {
        testSimpleReduceSideStreamingAfterFlatten(ExecType.MAPREDUCE);
    }
    
	private void testSimpleReduceSideStreamingAfterFlatten(ExecType execType) 
	throws Exception {
		PigServer pigServer = createPigServer(execType);

		File input = Util.createInputFile("tmp", "", 
				                          new String[] {"A,1", "B,2", "C,3", "D,2",
				                                        "A,5", "B,5", "C,8", "A,8",
				                                        "D,8", "A,9"});

		// Expected results
		String[] expectedFirstFields = new String[] {"A", "A", "A", "B", "C", "D"};
		int[] expectedSecondFields = new int[] {5, 8, 9, 5, 8, 8};
		Tuple[] expectedResults = 
			setupExpectedResults(expectedFirstFields, expectedSecondFields);

		// Pig query to run
		pigServer.registerQuery("IP = load 'file:" + input + "' using " + 
				                PigStorage.class.getName() + "(',');");
		pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
		pigServer.registerQuery("GROUPED_DATA = group FILTERED_DATA by $0;");
		pigServer.registerQuery("FLATTENED_GROUPED_DATA = foreach GROUPED_DATA " +
				                "generate flatten($1);");
        pigServer.registerQuery("S1 = stream FLATTENED_GROUPED_DATA through `" +
                                simpleEchoStreamingCommand + "`;");
		pigServer.registerQuery("OP = stream S1 through `" +
				                simpleEchoStreamingCommand + "`;");
		
		// Run the query and check the results
		Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
	}

	@Test
    public void testLocalSimpleOrderedReduceSideStreamingAfterFlatten() 
	throws Exception {
	    testSimpleOrderedReduceSideStreamingAfterFlatten(ExecType.LOCAL);
	}
	
    @Test
    public void testMRSimpleOrderedReduceSideStreamingAfterFlatten() 
    throws Exception {
        testSimpleOrderedReduceSideStreamingAfterFlatten(ExecType.MAPREDUCE);
    }
    
	private void testSimpleOrderedReduceSideStreamingAfterFlatten(
	        ExecType execType) throws Exception {
	    PigServer pigServer = createPigServer(execType);

		File input = Util.createInputFile("tmp", "", 
				                          new String[] {"A,1,2,3", "B,2,4,5",
				                                        "C,3,1,2", "D,2,5,2",
				                                        "A,5,5,1", "B,5,7,4",
				                                        "C,8,9,2", "A,8,4,5",
				                                        "D,8,8,3", "A,9,2,5"}
		                                 );

		// Expected results
		String[] expectedFirstFields = 
			new String[] {"A", "A", "A", "A", "B", "B", "C", "C", "D", "D"};
		int[] expectedSecondFields = new int[] {1, 9, 8, 5, 2, 5, 3, 8, 2, 8};
		int[] expectedThirdFields = new int[] {2, 2, 4, 5, 4, 7, 1, 9, 5, 8};
		int[] expectedFourthFields = new int[] {3, 5, 5, 1, 5, 4, 2, 2, 2, 3};
		Tuple[] expectedResults = new Tuple[10];
		for (int i = 0; i < expectedResults.length; ++i) {
			expectedResults[i] = new Tuple(4);
			expectedResults[i].setField(0, expectedFirstFields[i]);
			expectedResults[i].setField(1, expectedSecondFields[i]);
			expectedResults[i].setField(2, expectedThirdFields[i]);
			expectedResults[i].setField(3, expectedFourthFields[i]);
		}
			setupExpectedResults(expectedFirstFields, expectedSecondFields);

		// Pig query to run
		pigServer.registerQuery("IP = load 'file:" + input + "' using " + 
				                PigStorage.class.getName() + "(',');");
		pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("S1 = stream FILTERED_DATA through `" +
                                simpleEchoStreamingCommand + "`;");
        pigServer.registerQuery("S2 = stream S1 through `" +
                                simpleEchoStreamingCommand + "`;");
		pigServer.registerQuery("GROUPED_DATA = group IP by $0;");
		pigServer.registerQuery("ORDERED_DATA = foreach GROUPED_DATA { " +
				                "  D = order IP BY $2, $3;" +
                                "  generate flatten(D);" +
                                "};");
        pigServer.registerQuery("S3 = stream ORDERED_DATA through `" +
                                simpleEchoStreamingCommand + "`;");
		pigServer.registerQuery("OP = stream S3 through `" +
				                simpleEchoStreamingCommand + "`;");
		
		// Run the query and check the results
		Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
	}

    @Test
    public void testInputShipSpecs() throws Exception {
        PigServer pigServer = new PigServer(MAPREDUCE, cluster.getProperties());

        File input = Util.createInputFile("tmp", "", 
                                          new String[] {"A,1", "B,2", "C,3", 
                                                        "D,2", "A,5", "B,5", 
                                                        "C,8", "A,8", "D,8", 
                                                        "A,9"});

        // Perl script 
        String[] script = 
            new String[] {
                          "#!/usr/bin/perl",
                          "open(INFILE,  $ARGV[0]) or die \"Can't open \".$ARGV[0].\"!: $!\";",
                          "while (<INFILE>) {",
                          "  chomp $_;",
                          "  print STDOUT \"$_\n\";",
                          "  print STDERR \"STDERR: $_\n\";",
                          "}",
                         };
        File command = Util.createInputFile("script", "pl", script);
        
        // Expected results
        String[] expectedFirstFields = 
            new String[] {"A", "B", "C", "A", "D", "A"};
        int[] expectedSecondFields = new int[] {5, 5, 8, 8, 8, 9};
        Tuple[] expectedResults = 
            setupExpectedResults(expectedFirstFields, expectedSecondFields);

        // Pig query to run
        pigServer.registerQuery(
                "define CMD `" + command.getName() + " foo` " +
                "ship ('" + command + "') " +
                "input('foo' using " + PigStorage.class.getName() + "(',')) " +
                "stderr();"); 

        pigServer.registerQuery("IP = load 'file:" + input + "' using " + 
                                PigStorage.class.getName() + "(',');");
        pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("OP = stream FILTERED_DATA through CMD;");

        String output = "/pig/out";
        pigServer.deleteFile(output);
        pigServer.store("OP", output, PigStorage.class.getName() + "(',')");
        
        InputStream op = FileLocalizer.open(output, pigServer.getPigContext());
        PigStorage ps = new PigStorage(",");
        ps.bindTo("", new BufferedPositionedInputStream(op), 0, Long.MAX_VALUE); 
        List<Tuple> outputs = new ArrayList<Tuple>();
        Tuple t;
        while ((t = ps.getNext()) != null) {
            outputs.add(t);
        }

        // Run the query and check the results
        Util.checkQueryOutputs(outputs.iterator(), expectedResults);
    }

    @Test
	public void testOutputShipSpecs() throws Exception {
	    PigServer pigServer = new PigServer(MAPREDUCE, cluster.getProperties());

	    File input = Util.createInputFile("tmp", "", 
	                                      new String[] {"A,1", "B,2", "C,3", 
	                                                    "D,2", "A,5", "B,5", 
	                                                    "C,8", "A,8", "D,8", 
	                                                    "A,9"});

	    // Perl script 
	    String[] script = 
	        new String[] {
	                      "#!/usr/bin/perl",
                          "open(OUTFILE, \">\", $ARGV[0]) or die \"Can't open \".$ARGV[1].\"!: $!\";",
                          "open(OUTFILE2, \">\", $ARGV[1]) or die \"Can't open \".$ARGV[2].\"!: $!\";",
                          "while (<STDIN>) {",
                          "  print OUTFILE \"A,10\n\";",
                          "  print STDERR \"STDERR: $_\n\";",
                          "  print OUTFILE2 \"Secondary Output: $_\n\";",
                          "}",
	                     };
	    File command = Util.createInputFile("script", "pl", script);

        // Expected results
        String[] expectedFirstFields = 
            new String[] {"A", "A", "A", "A", "A", "A"};
        int[] expectedSecondFields = new int[] {10, 10, 10, 10, 10, 10};
        Tuple[] expectedResults = 
            setupExpectedResults(expectedFirstFields, expectedSecondFields);

        // Pig query to run
        pigServer.registerQuery(
                "define CMD `" + command.getName() + " foo bar` " +
                "ship ('" + command + "') " +
        		"output('foo' using " + PigStorage.class.getName() + "(','), " +
        		"'bar' using " + PigStorage.class.getName() + "(',')) " +
        		"stderr();"); 
        pigServer.registerQuery("IP = load 'file:" + input + "' using " + 
                                PigStorage.class.getName() + "(',');");
        pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("OP = stream FILTERED_DATA through CMD;");
        
        String output = "/pig/out";
        pigServer.deleteFile(output);
        pigServer.store("OP", output, PigStorage.class.getName() + "(',')");
        
        InputStream op = FileLocalizer.open(output, pigServer.getPigContext());
        PigStorage ps = new PigStorage(",");
        ps.bindTo("", new BufferedPositionedInputStream(op), 0, Long.MAX_VALUE); 
        List<Tuple> outputs = new ArrayList<Tuple>();
        Tuple t;
        while ((t = ps.getNext()) != null) {
            outputs.add(t);
        }

        // Run the query and check the results
        Util.checkQueryOutputs(outputs.iterator(), expectedResults);
    }

    @Test
    public void testInputOutputSpecs() throws Exception {
        PigServer pigServer = new PigServer(MAPREDUCE, cluster.getProperties());

        File input = Util.createInputFile("tmp", "", 
                                          new String[] {"A,1", "B,2", "C,3", 
                                                        "D,2", "A,5", "B,5", 
                                                        "C,8", "A,8", "D,8", 
                                                        "A,9"});

        // Perl script 
        String[] script = 
            new String[] {
                          "#!/usr/bin/perl",
                          "open(INFILE,  $ARGV[0]) or die \"Can't open \".$ARGV[0].\"!: $!\";",
                          "open(OUTFILE, \">\", $ARGV[1]) or die \"Can't open \".$ARGV[1].\"!: $!\";",
                          "open(OUTFILE2, \">\", $ARGV[2]) or die \"Can't open \".$ARGV[2].\"!: $!\";",
                          "while (<INFILE>) {",
                          "  chomp $_;",
                          "  print OUTFILE \"$_\n\";",
                          "  print STDERR \"STDERR: $_\n\";",
                          "  print OUTFILE2 \"Secondary Output: $_\n\";",
                          "}",
                         };
        File command = Util.createInputFile("script", "pl", script);

        // Expected results
        String[] expectedFirstFields = 
            new String[] {"A", "B", "C", "A", "D", "A"};
        int[] expectedSecondFields = new int[] {5, 5, 8, 8, 8, 9};
        Tuple[] expectedResults = 
            setupExpectedResults(expectedFirstFields, expectedSecondFields);

        // Pig query to run
        pigServer.registerQuery(
                "define CMD `" + command.getName() + " foo bar foobar` " +
                "ship ('" + command + "') " +
                "input('foo' using " + PigStorage.class.getName() + "(',')) " +
                "output('bar' using " + PigStorage.class.getName() + "(','), " +
                "'foobar' using " + PigStorage.class.getName() + "(',')) " +
                "stderr();"); 
        pigServer.registerQuery("IP = load 'file:" + input + "' using " + 
                                PigStorage.class.getName() + "(',');");
        pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("OP = stream FILTERED_DATA through CMD;");
        
        String output = "/pig/out";
        pigServer.deleteFile(output);
        pigServer.store("OP", output, PigStorage.class.getName() + "(',')");
        
        InputStream op = FileLocalizer.open(output, pigServer.getPigContext());
        PigStorage ps = new PigStorage(",");
        ps.bindTo("", new BufferedPositionedInputStream(op), 0, Long.MAX_VALUE); 
        List<Tuple> outputs = new ArrayList<Tuple>();
        Tuple t;
        while ((t = ps.getNext()) != null) {
            outputs.add(t);
        }

        // Run the query and check the results
        Util.checkQueryOutputs(outputs.iterator(), expectedResults);
        
        // Cleanup
        pigServer.deleteFile(output);
    }

    @Test
    public void testLocalSimpleMapSideStreamingWithUnixPipes() 
    throws Exception {
        testSimpleMapSideStreamingWithUnixPipes(ExecType.LOCAL);
    }
    
    @Test
    public void testMRSimpleMapSideStreamingWithUnixPipes() throws Exception {
        testSimpleMapSideStreamingWithUnixPipes(ExecType.MAPREDUCE);
    }
    
    private void testSimpleMapSideStreamingWithUnixPipes(ExecType execType) 
    throws Exception {
        PigServer pigServer = createPigServer(execType);
        File input = Util.createInputFile("tmp", "", 
                                          new String[] {"A,1", "B,2", "C,3", "D,2",
                                                        "A,5", "B,5", "C,8", "A,8",
                                                        "D,8", "A,9"});

        // Expected results
        String[] expectedFirstFields = 
            new String[] {"A", "B", "C", "D", "A", "B", "C", "A", "D", "A"};
        int[] expectedSecondFields = new int[] {1, 2, 3, 2, 5, 5, 8, 8, 8, 9};
        Tuple[] expectedResults = 
            setupExpectedResults(expectedFirstFields, expectedSecondFields);

        // Pig query to run
        pigServer.registerQuery("define CMD `" + simpleEchoStreamingCommand + 
                                " | " + simpleEchoStreamingCommand + "`;");
        pigServer.registerQuery("IP = load 'file:" + input + "' using " + 
                                PigStorage.class.getName() + "(',');");
        pigServer.registerQuery("OP = stream IP through CMD;");
        
        // Run the query and check the results
        Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
    }

    @Test
    public void testLocalNegativeLoadStoreOptimization() throws Exception {
        testNegativeLoadStoreOptimization(ExecType.LOCAL);
    }
    
    @Test
    public void testMRNegativeLoadStoreOptimization() throws Exception {
        testNegativeLoadStoreOptimization(ExecType.MAPREDUCE);
    }
    
    private void testNegativeLoadStoreOptimization(ExecType execType) 
    throws Exception {
            PigServer pigServer = createPigServer(execType);
        File input = Util.createInputFile("tmp", "", 
                                          new String[] {"A,1", "B,2", "C,3", "D,2",
                                                        "A,5", "B,5", "C,8", "A,8",
                                                        "D,8", "A,9"});

        // Expected results
        String[] expectedFirstFields = new String[] {"A", "B", "C", "A", "D", "A"};
        int[] expectedSecondFields = new int[] {5, 5, 8, 8, 8, 9};
        Tuple[] expectedResults = 
            setupExpectedResults(expectedFirstFields, expectedSecondFields);

        // Pig query to run
        pigServer.registerQuery("define CMD `"+ simpleEchoStreamingCommand + 
                                "` input(stdin using PigDump());");
        pigServer.registerQuery("IP = load 'file:" + input + "' using " + 
                                PigStorage.class.getName() + "(',') " +
                                "split by 'file';");
        pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("OP = stream FILTERED_DATA through `" +
                                simpleEchoStreamingCommand + "`;");
        
        // Run the query and check the results
        Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
    }

}
