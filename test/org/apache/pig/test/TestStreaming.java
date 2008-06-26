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

import org.apache.pig.PigServer.ExecType;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.Assert;
import org.junit.Test;

public class TestStreaming extends PigExecTestCase {

	private static final String simpleEchoStreamingCommand;
        static {
            if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
                simpleEchoStreamingCommand = "perl -ne 'print \\\"$_\\\"'";
            else
                simpleEchoStreamingCommand = "perl -ne 'print \"$_\"'";
        }

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
	public void testSimpleMapSideStreaming() 
	throws Exception {
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
		pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
				                PigStorage.class.getName() + "(',');");
		pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("S1 = stream FILTERED_DATA through `" +
                                simpleEchoStreamingCommand + "`;");
		pigServer.registerQuery("OP = stream S1 through `" +
				                simpleEchoStreamingCommand + "`;");
		
		// Run the query and check the results
		Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
	}
    
	@Test
	public void testSimpleMapSideStreamingWithOutputSchema() 
	throws Exception {
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
		pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
				                PigStorage.class.getName() + "(',');");
		pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
		pigServer.registerQuery("STREAMED_DATA = stream FILTERED_DATA through `" +
				                simpleEchoStreamingCommand + "` as (f0, f1);");
		pigServer.registerQuery("OP = filter STREAMED_DATA by f1 > '6';");
		
		// Run the query and check the results
		Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
	}

	@Test
	public void testSimpleReduceSideStreamingAfterFlatten() 
	throws Exception {
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
		pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
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
	public void testSimpleOrderedReduceSideStreamingAfterFlatten(
	        ExecType execType) throws Exception {
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
		pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
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
        // FIXME : this should be tested in all modes
        if(execType == ExecType.LOCAL)
            return;
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
        File command1 = Util.createInputFile("script", "pl", script);
        File command2 = Util.createInputFile("script", "pl", script);
        
        // Expected results
        String[] expectedFirstFields = 
            new String[] {"A", "B", "C", "A", "D", "A"};
        int[] expectedSecondFields = new int[] {5, 5, 8, 8, 8, 9};
        Tuple[] expectedResults = 
            setupExpectedResults(expectedFirstFields, expectedSecondFields);

        // Pig query to run
        pigServer.registerQuery(
                "define CMD1 `" + command1.getName() + " foo` " +
                "ship ('" + Util.encodeEscape(command1.toString()) + "') " +
                "input('foo' using " + PigStorage.class.getName() + "(',')) " +
                "stderr();"); 
        pigServer.registerQuery(
                "define CMD2 `" + command2.getName() + " bar` " +
                "ship ('" + Util.encodeEscape(command2.toString()) + "') " +
                "input('bar' using " + PigStorage.class.getName() + "(',')) " +
                "stderr();"); 
        pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
                                PigStorage.class.getName() + "(',');");
        pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("STREAMED_DATA = stream FILTERED_DATA " +
        		                "through CMD1;");
        pigServer.registerQuery("OP = stream STREAMED_DATA through CMD2;");

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
    public void testInputCacheSpecs() throws Exception {
        // Can't run this without HDFS
        if(execType == ExecType.LOCAL)
            return;
        
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
        // Copy the scripts to HDFS
        File command1 = Util.createInputFile("script", "pl", script);
        File command2 = Util.createInputFile("script", "pl", script);
        String c1 = FileLocalizer.hadoopify(command1.toString(), 
                                            pigServer.getPigContext());
        String c2 = FileLocalizer.hadoopify(command2.toString(), 
                                            pigServer.getPigContext());
        
        // Expected results
        String[] expectedFirstFields = 
            new String[] {"A", "B", "C", "A", "D", "A"};
        int[] expectedSecondFields = new int[] {5, 5, 8, 8, 8, 9};
        Tuple[] expectedResults = 
            setupExpectedResults(expectedFirstFields, expectedSecondFields);

        // Pig query to run
        pigServer.registerQuery(
                "define CMD1 `script1.pl foo` " +
                "cache ('" + c1 + "#script1.pl') " +
                "input('foo' using " + PigStorage.class.getName() + "(',')) " +
                "stderr();"); 
        pigServer.registerQuery(
                "define CMD2 `script2.pl bar` " +
                "cache ('" + c2 + "#script2.pl') " +
                "input('bar' using " + PigStorage.class.getName() + "(',')) " +
                "stderr();"); 
        pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
                                PigStorage.class.getName() + "(',');");
        pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("STREAMED_DATA = stream FILTERED_DATA " +
                                "through CMD1;");
        pigServer.registerQuery("OP = stream STREAMED_DATA through CMD2;");

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
        // FIXME : this should be tested in all modes
        if(execType == ExecType.LOCAL)
            return;
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
                          "  print OUTFILE \"$_\n\";",
                          "  print STDERR \"STDERR: $_\n\";",
                          "  print OUTFILE2 \"A,10\n\";",
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
                "ship ('" + Util.encodeEscape(command.toString()) + "') " +
        		"output('foo' using " + PigStorage.class.getName() + "(','), " +
        		"'bar' using " + PigStorage.class.getName() + "(',')) " +
        		"stderr();"); 
        pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
                                PigStorage.class.getName() + "(',');");
        pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("OP = stream FILTERED_DATA through CMD;");
        
        String output = "/pig/out";
        pigServer.deleteFile(output);
        pigServer.store("OP", output, PigStorage.class.getName() + "(',')");
        
        InputStream op = FileLocalizer.open(output+"/bar", 
                                            pigServer.getPigContext());
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
        // FIXME : this should be tested in all modes
        if(execType == ExecType.LOCAL)
            return;
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
                          "  print OUTFILE2 \"$_\n\";",
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
                "ship ('" + Util.encodeEscape(command.toString()) + "') " +
                "input('foo' using " + PigStorage.class.getName() + "(',')) " +
                "output('bar', " +
                "'foobar' using " + PigStorage.class.getName() + "(',')) " +
                "stderr();"); 
        pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
                                PigStorage.class.getName() + "(',');");
        pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("OP = stream FILTERED_DATA through CMD;");
        
        String output = "/pig/out";
        pigServer.deleteFile(output);
        pigServer.store("OP", output, PigStorage.class.getName() + "(',')");
        
        InputStream op = FileLocalizer.open(output+"/foobar", 
                                            pigServer.getPigContext());
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
    public void testSimpleMapSideStreamingWithUnixPipes() 
    throws Exception {
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
        pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
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
                                "` input(stdin using PigDump);");
        pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
                                PigStorage.class.getName() + "(',') " +
                                "split by 'file';");
        pigServer.registerQuery("FILTERED_DATA = filter IP by $1 > '3';");
        pigServer.registerQuery("OP = stream FILTERED_DATA through `" +
                                simpleEchoStreamingCommand + "`;");
        
        // Run the query and check the results
        Util.checkQueryOutputs(pigServer.openIterator("OP"), expectedResults);
    }

}
