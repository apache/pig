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
package org.apache.pig.test.pigmix.datagen;

import java.io.*;
import java.lang.SecurityException;
import java.text.ParseException;
import java.util.*;

import sdsu.algorithms.data.Zipf;

import org.apache.pig.tools.cmdline.CmdLineParser;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * A tool to generate data for performance testing.
 */
public class DataGenerator extends Configured implements Tool {
    ColSpec[] colSpecs;
    long seed = -1;
    long numRows = -1;
    int numMappers = -1;
    String outputFile;
    String inFile;
    char separator = '\u0001' ;
    Random rand;

    private String[] mapkey = { "a", "b", "c", "d", "e", "f", "g", "h", "i",
        "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w",
        "x", "y", "z"};

    public static void main(String[] args) throws Exception {    	
    	DataGenerator dg = new DataGenerator();    	 
    	try {
    		ToolRunner.run(new Configuration(), dg, args);    			
   		}catch(Exception e) {
    		throw new IOException (e);
    	}    	
        dg.go();
    }
    
    protected DataGenerator(long seed) {    
    	System.out.println("Using seed " + seed);
        rand = new Random(seed);
    }

    protected DataGenerator() {
    	
    }
    
    protected DataGenerator(String[] args) {
    	
    }
    
    public int run(String[] args) throws Exception {    	
        CmdLineParser opts = new CmdLineParser(args);
        opts.registerOpt('e', "seed", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('f', "file", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('r', "rows", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('s', "separator", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('i', "input", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('m', "mappers", CmdLineParser.ValueExpected.OPTIONAL);

        char opt;
        try {
            while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
                switch (opt) {
                case 'e':
                    seed = Long.valueOf(opts.getValStr());
                    break;

                case 'f':
                    outputFile = opts.getValStr();                   
                    break;

                case 'i':
                    inFile = opts.getValStr();                   
                    break;

                case 'r':
                    numRows = Long.valueOf(opts.getValStr());
                    break;
    
                case 's':
                    separator = opts.getValStr().charAt(0);
                    break;
                    
                case 'm':
                	numMappers = Integer.valueOf(opts.getValStr());
                	break;

                default:
                    usage();
                    break;
                }
            }
        } catch (ParseException pe) {
            System.err.println("Couldn't parse the command line arguments, " +
                pe.getMessage());
            usage();
        }

        if (numRows < 1 && inFile == null) usage();

        if (numRows > 0 && inFile != null) usage();              
        
        if (numMappers > 0 && seed != -1) usage();
        
        if (seed == -1){
        	seed = System.currentTimeMillis();
        }
        
        String remainders[] = opts.getRemainingArgs();
        colSpecs = new ColSpec[remainders.length];
        for (int i = 0; i < remainders.length; i++) {
            colSpecs[i] = new ColSpec(remainders[i]);
        }
        System.err.println("Using seed " + seed);
        rand = new Random(seed); 
        
        return 0;
    }

    private void go() throws IOException {    	    
    	long t1 = System.currentTimeMillis();
    	if (numMappers <= 0) {
    		System.out.println("Generate data in local mode.");
        	goLocal();
        }else{
        	System.out.println("Generate data in hadoop mode.");        
        	HadoopRunner runner = new HadoopRunner();
        	runner.goHadoop();
        }
    	   	
    	long t2 = System.currentTimeMillis();
    	System.out.println("Job is successful! It took " + (t2-t1)/1000 + " seconds.");
    }
    
    public void goLocal() throws IOException {
    	
    	PrintWriter out = null;
		try {
            out = new PrintWriter(outputFile);
        } catch (FileNotFoundException fnfe) {
            System.err.println("Could not find file " + outputFile +
                ", " + fnfe.getMessage());
            return;
        } catch (SecurityException se) {
            System.err.println("Could not write to file " + outputFile +
                ", " + se.getMessage());
            return;
        }
        
        BufferedReader in = null;
        if (inFile != null) {
        	try {
      	  		in = new BufferedReader(new FileReader(inFile));
        	} catch (FileNotFoundException fnfe) {
        		System.err.println("Unable to find input file " + inFile);
        		return;
        	}
        }
        
        if (numRows > 0) {
        	for (int i = 0; i < numRows; i++) {
        		writeLine(out);
        		out.println();
        	}
        } else if (in != null) {
        	String line;
        	while ((line = in.readLine()) != null) {
        		out.print(line);
        		writeLine(out);
        		out.println();
        	}
        }
        out.close();
    }      
    
    protected void writeLine(PrintWriter out) {
        for (int j = 0; j < colSpecs.length; j++) {
            if (j != 0) out.print(separator);
            // First, decide if it's going to be null            
            if (rand.nextInt(100) < colSpecs[j].pctNull) {
            	continue;
            }
            writeCol(colSpecs[j], out);
        }
    }

    private void writeCol(ColSpec colspec, PrintWriter out) {
        switch (colspec.datatype) {
        case INT:
            out.print(colspec.nextInt());
            break;

        case LONG:
            out.print(colspec.nextLong());
            break;

        case FLOAT:
            out.print(colspec.nextFloat());
            break;

        case DOUBLE:
            out.print(colspec.nextDouble());
            break;

        case STRING:
            out.print(colspec.nextString());
            break;

        case MAP:
            int len = rand.nextInt(20) + 6;
            for (int k = 0; k < len; k++) {
                if (k != 0) out.print('');
                out.print(mapkey[k] + '');
                out.print(colspec.gen.randomString());
            }
            break;

        case BAG:
            int numElements = rand.nextInt(5) + 5;
            for (int i = 0; i < numElements; i++) {
                if (i != 0) out.print('');
                switch(colspec.contained.datatype) {
                    case INT: out.print("i"); break;
                    case LONG: out.print("l"); break;
                    case FLOAT: out.print("f"); break;
                    case DOUBLE: out.print("d"); break;
                    case STRING: out.print("s"); break;
                    case MAP: out.print("m"); break;
                    case BAG: out.print("b"); break;
                    default: throw new RuntimeException("should never be here");
                }
                writeCol(colspec.contained, out);
            }
        }
    }

    private void usage() {
        System.err.println("Usage: datagen -rows numrows [options] colspec ...");
        System.err.println("\tOptions:");
        System.err.println("\t-e -seed seed value for random numbers");
        System.err.println("\t-f -file output file, default is stdout");
        System.err.println("\t-i -input input file, lines will be read from");
        System.err.println("\t\tthe file and additional columns appended.");
        System.err.println("\t\tMutually exclusive with -r.");
        System.err.println("\t-r -rows number of rows to output");
        System.err.println("\t-s -separator character, default is ^A");
        System.err.println("\t-m -number of mappers to run concurrently to generate data. " +
        		"If not specified, DataGenerator runs locally. This option can NOT be used with -e.");
        System.err.println();
        System.err.print("\tcolspec: columntype:average_size:cardinality:");
        System.err.println("distribution_type:percent_null");
        System.err.println("\tcolumntype:");
        System.err.println("\t\ti = int");
        System.err.println("\t\tl = long");
        System.err.println("\t\tf = float");
        System.err.println("\t\td = double");
        System.err.println("\t\ts = string");
        System.err.println("\t\tm = map");
        System.err.println("\t\tbx = bag of x, where x is a columntype");
        System.err.println("\tdistribution_type:");
        System.err.println("\t\tu = uniform");
        System.err.println("\t\tz = zipf");

        throw new RuntimeException();
    }


 
    static enum Datatype { INT, LONG, FLOAT, DOUBLE, STRING, MAP, BAG };
    static enum DistributionType { UNIFORM, ZIPF };
    protected class ColSpec {
    	String arg;
        Datatype datatype;
        DistributionType distype;
        int avgsz;
        int card;
        RandomGenerator gen;
        int pctNull;
        ColSpec contained;
        String mapfile;
        Map<Integer, Object> map;

        public ColSpec(String arg) {
        	this.arg = arg;
        	
            String[] parts = arg.split(":");
            if (parts.length != 5 && parts.length != 6) {
                System.err.println("Colspec [" + arg + "] format incorrect"); 
                usage();
            }

            switch (parts[0].charAt(0)) {
                case 'i': datatype = Datatype.INT; break;
                case 'l': datatype = Datatype.LONG; break;
                case 'f': datatype = Datatype.FLOAT; break;
                case 'd': datatype = Datatype.DOUBLE; break;
                case 's': datatype = Datatype.STRING; break;
                case 'm': datatype = Datatype.MAP; break;
                case 'b':
                    datatype = Datatype.BAG;
                    contained = new ColSpec(arg.substring(1));
                    return;
                default: 
                    System.err.println("Don't know column type " +
                        parts[0].charAt(0));
                    usage();
                    break;
            }
            avgsz = Integer.valueOf(parts[1]);
            card = Integer.valueOf(parts[2]);
            switch (parts[3].charAt(0)) {
                case 'u': 
                    gen = new UniformRandomGenerator(avgsz, card);
                    distype = DistributionType.UNIFORM;
                    break;

                case 'z': 
                    gen = new ZipfRandomGenerator(avgsz, card);
                    distype = DistributionType.ZIPF;
                    break;

                default:
                    System.err.println("Don't know generator type " +
                        parts[3].charAt(0));
                    usage();
                    break;
            }

            pctNull = Integer.valueOf(parts[4]);
            if (pctNull > 100) {
                System.err.println("Percentage null must be between 0-100, "
                    + "you gave" + pctNull);
                usage();
            }
            contained = null;

            // if config has 6 columns, the last col is the file name 
            // of the mapping file from random number to field value
            if (parts.length == 6) {
            	mapfile = parts[5];
            	gen.hasMapFile = true;
            }
            
            map = new HashMap<Integer, Object>();
        }
        
        public int nextInt() {
        	return gen.nextInt(map);
        }
        
        public long nextLong() {
        	return gen.nextInt(map);
        }
        
        public double nextDouble() {
        	return gen.nextDouble(map);
        }
        
        public float nextFloat() {
        	return gen.nextFloat(map);
        }
        
        public String nextString() {
        	return gen.nextString(map);
        }
    }

    abstract class RandomGenerator {

        protected int avgsz;
        protected boolean hasMapFile; // indicating whether a map file from 
                                      // random number to the field value is pre-defined

        abstract public int nextInt(Map<Integer, Object> map);
        abstract public long nextLong(Map<Integer, Object> map);
        abstract public float nextFloat(Map<Integer, Object> map);
        abstract public double nextDouble(Map<Integer, Object> map);
        abstract public String nextString(Map<Integer, Object> map);

        public String randomString() {
            int var = (int)((double)avgsz * 0.3);
            StringBuffer sb = new StringBuffer(avgsz + var);
            if (var < 1) var = 1;
            int len = rand.nextInt(2 * var) + avgsz - var;
            for (int i = 0; i < len; i++) {
                int n = rand.nextInt(122 - 65) + 65;
                sb.append(Character.toChars(n));
            }
            return sb.toString();
        }
        
        public float randomFloat() {
        	return rand.nextFloat() * rand.nextInt();
        }
        
        public double randomDouble() {
        	return rand.nextDouble() * rand.nextInt();
        }
    }

    class UniformRandomGenerator extends RandomGenerator {
        int card;       
        
        public UniformRandomGenerator(int a, int c) {
            avgsz = a;
            card = c;            
        }

        public int nextInt(Map<Integer, Object> map) {
            return rand.nextInt(card);
        }

        public long nextLong(Map<Integer, Object> map) {
            return rand.nextLong() % card;
        }

        public float nextFloat(Map<Integer, Object> map) {
            int seed = rand.nextInt(card);
            Float f = (Float)map.get(seed);
            if (f == null) {
            	if (!hasMapFile) {
            		f = randomFloat();
            		map.put(seed, f);
            	}else{
            		throw new IllegalStateException("Number " + seed + " is not found in map file");
            	}
            }
            return f;
        }
        
        public double nextDouble(Map<Integer, Object> map) {
            int seed = rand.nextInt(card);
            Double d = (Double)map.get(seed);
            if (d == null) {
            	if (!hasMapFile) {
            		d = randomDouble();
            		map.put(seed, d);
            	}else{
            		throw new IllegalStateException("Number " + seed + " is not found in map file");
            	}
            }
            return d;
        }

        public String nextString(Map<Integer, Object> map) {
            int seed = rand.nextInt(card);
            String s = (String)map.get(seed);
            if (s == null) {
            	if (!hasMapFile) {
            		s = randomString();
            		map.put(seed, s);
            	}else{
            		throw new IllegalStateException("Number " + seed + " is not found in map file");
            	}
            }
            return s;
        }
        
    }

    class ZipfRandomGenerator extends RandomGenerator {
        Zipf z;

        public ZipfRandomGenerator(int a, int c) {
            avgsz = a;
            z = new Zipf(c);         
        }
        
        
        // the Zipf library returns a random number [1..cardinality], so we substract by 1
        // to get [0..cardinality)
        // the randome number returned by zipf library is an integer, but converted into double
        private double next() {
        	return z.nextElement()-1;
        }

        public int nextInt(Map<Integer, Object> map) {
            return (int)next();
        }

        public long nextLong(Map<Integer, Object> map) {
            return (long)next();
        }

        public float nextFloat(Map<Integer, Object> map) {
        	int seed = (int)next();
            Float d = (Float)map.get(seed);
            if (d == null) {
            	if (!hasMapFile) {
            		d = randomFloat();
            		map.put(seed, d);
            	}else{
            		throw new IllegalStateException("Number " + seed + " is not found in map file");
            	}
            }
            return d;
        }

        public double nextDouble(Map<Integer, Object> map) {
        	 int seed = (int)next();
             Double d = (Double)map.get(seed);
             if (d == null) {
             	if (!hasMapFile) {
             		d = randomDouble();
             		map.put(seed, d);
             	}else{
             		throw new IllegalStateException("Number " + seed + " is not found in map file");
             	}
             }
             return d;
        }
        
        public String nextString(Map<Integer, Object> map) {
            int seed = (int)next();
            String s = (String)map.get(seed);
            if (s == null) {
            	if (!hasMapFile) {
            		s = randomString();
            		map.put(seed, s);
            	}else{
            		throw new IllegalStateException("Number " + seed + " is not found in map file");
            	}
            }
            return s;
        }
    }
    
//  launch hadoop job
    class HadoopRunner {
    	Random r;
    	FileSystem fs;
    	Path tmpHome;
    	
    	public HadoopRunner() {
    		r = new Random();
    	}
    	
    	public void goHadoop() throws IOException {
            // Configuration processed by ToolRunner
            Configuration conf = getConf();
            
            // Create a JobConf using the processed conf            
            JobConf job = new JobConf(conf);     
            fs = FileSystem.get(job);           
            
            tmpHome = createTempDir(null);                          
            
            String config = genMapFiles().toUri().getRawPath();
            // set config properties into job conf
            job.set("fieldconfig", config);      
            job.set("separator", String.valueOf((int)separator));
            
            
            job.setJobName("data-gen");
            job.setNumMapTasks(numMappers);
            job.setNumReduceTasks(0);          
            job.setMapperClass(DataGenMapper.class);   
            job.setJarByClass(DataGenMapper.class);
            
            // if inFile is specified, use it as input
            if (inFile != null) {
           	 FileInputFormat.setInputPaths(job, inFile);
           	 job.set("hasinput", "true");
           } else {
        	   job.set("hasinput", "false");
        	   Path input = genInputFiles();        
        	   FileInputFormat.setInputPaths(job, input);
           }
           FileOutputFormat.setOutputPath(job, new Path(outputFile));
                               
            // Submit the job, then poll for progress until the job is complete
            System.out.println("Submit hadoop job...");
            RunningJob j = JobClient.runJob(job);
            if (!j.isSuccessful()) {
            	throw new IOException("Job failed");
            }
                        
            if (fs.exists(tmpHome)) {            	
            	fs.delete(tmpHome, true);
            }
         }
    	
    	 private Path genInputFiles() throws IOException {
    		 long avgRows = numRows/numMappers;
    	        
    		 // create a temp directory as mappers input
    	     Path input = createTempDir(tmpHome);
    	     System.out.println("Generating input files into " + input.toString());
    	     
    	     long rowsLeft = numRows;
    	     
    	     // create one input file per mapper, which contains
    	     // the number of rows 
    	     for(int i=0; i<numMappers; i++) {
    	    	 Object[] tmp = createTempFile(input, false);
    	    	 PrintWriter pw = new PrintWriter((OutputStream)tmp[1]);
    	    	 
    	    	 if (i < numMappers-1) {
    	    		 pw.println(avgRows);
    	    	 }else{
    	    		 // last mapper takes all the rows left
    	    		 pw.println(rowsLeft);
    	    	 }
    	    
    	    	 pw.close();
    	    	 rowsLeft -= avgRows;
    	     }
    	     
    	     return input;
    	 }
    	
    	// generate map files for all the fields that need to pre-generate map files
    	// return a config file which contains config info for each field, including 
    	// the path to their map file
    	 private Path genMapFiles() throws IOException {
    		 Object[] tmp = createTempFile(tmpHome, false);
    		 
    		 System.out.println("Generating column config file in " + tmp[0].toString());
    		 PrintWriter pw = new PrintWriter((OutputStream)tmp[1]);
    		 for(int i=0; i<colSpecs.length; i++) {
    			 DataGenerator.Datatype datatype = colSpecs[i].datatype;
    			 pw.print(colSpecs[i].arg);
    			 
    			 if ( datatype == DataGenerator.Datatype.FLOAT || datatype == DataGenerator.Datatype.DOUBLE ||
    					 datatype == DataGenerator.Datatype.STRING) 	 {
    				 Path p = genMapFile(colSpecs[i]);
    				 pw.print(':');    				
    				 pw.print(p.toUri().getRawPath());				 
    			 } 
    			 
    			 pw.println();
    		 }
    		 
    		 pw.close();
    		 
    		 return (Path)tmp[0];
    	 }
    	 
    	 // genereate a map file between random number to field value
    	 // return the path of the map file
    	 private Path genMapFile(DataGenerator.ColSpec col) throws IOException {
    		 int card = col.card;
    		 Object[] tmp = createTempFile(tmpHome, false);
    		 
    		 System.out.println("Generating mapping file for column " + col.arg + " into " + tmp[0].toString());
    		 PrintWriter pw = new PrintWriter((OutputStream)tmp[1]);
    		 HashSet<Object> hash = new HashSet<Object>(card);
    		 for(int i=0; i<card; i++) {
    			 pw.print(i);
    			 pw.print("\t");
    			 Object next = null;
    			 do {
    				 if (col.datatype == DataGenerator.Datatype.DOUBLE) {
        				 next = col.gen.randomDouble();
        			 }else if (col.datatype == DataGenerator.Datatype.FLOAT) {
        				 next = col.gen.randomFloat();
        			 }else if (col.datatype == DataGenerator.Datatype.STRING) {
        				 next = col.gen.randomString();
        			 }
    			 }while(hash.contains(next));
    			 
    			 hash.add(next);
    			 
    			 pw.println(next);
    			 
    			 if ( (i>0 && i%300000 == 0) || i == card-1 ) {
    				 System.out.println("processed " + i*100/card + "%." );
    				 pw.flush();
    			 }    			 
    		 }    		 
    		 
    		 pw.close();
    		 
    		 return (Path)tmp[0];
        	 
    	 }
    	 
    	 private Path createTempDir(Path parentDir) throws IOException {
    		 Object[] obj = createTempFile(parentDir, true);
    		 return (Path)obj[0];
    	 }
    	 
    	 private Object[] createTempFile(Path parentDir, boolean isDir) throws IOException {		
    		 Path tmp_home = parentDir;
    		 
    		 if (tmp_home == null) {
    			 tmp_home = new Path(fs.getHomeDirectory(), "tmp");
    		 }
    		 
    		 if (!fs.exists(tmp_home)) {
    			 fs.mkdirs(tmp_home);
    		 }
    		 
    		 int id = r.nextInt();
    		 Path f = new Path(tmp_home, "tmp" + id);
    		 while (fs.exists(f)) {
    			 id = r.nextInt();
    			 f = new Path(tmp_home, "tmp" + id);
    		 }
    		 
    		 // return a 2-element array. first element is PATH,
    		 // second element is OutputStream
    		 Object[] r = new Object[2];
    		 r[0] = f;
    		 if (!isDir) {
    			 r[1] = fs.create(f);
    		 }else{
    			 fs.mkdirs(f);
    		 }
    		 
    		 return r;
    	 }    	
    }

	 public static class DataGenMapper extends MapReduceBase implements Mapper<LongWritable, Text, String, String> {
 		private JobConf jobConf;
 		private DataGenerator dg; 		
 		private boolean hasInput;
 	   
 		public void configure(JobConf jobconf) {
 			this.jobConf = jobconf;
 						 			
 			int id = Integer.parseInt(jobconf.get("mapred.task.partition"));
 			long time = System.currentTimeMillis() - id*3600*24*1000;
 			 			
 			dg = new DataGenerator( ((time-id*3600*24*1000) | (id << 48)));
 			
 		    dg.separator = (char)Integer.parseInt(jobConf.get("separator"));
 		     		    
 		    if (jobConf.get("hasinput").equals("true")) {
 		    	hasInput = true;
 		    }
 		    
 		    String config = jobConf.get("fieldconfig");
 		    		    
 		    try {			    
 			    FileSystem fs = FileSystem.get(jobconf);
 			    
 			    // load in config file for each column
 			    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(config))));
 			    String line = null;
 			    List<DataGenerator.ColSpec> cols = new ArrayList<DataGenerator.ColSpec>();
 			    while((line = reader.readLine()) != null) { 			    	
 			    	cols.add(dg.new ColSpec(line));		    	
 			    }
 			    reader.close();
 			    dg.colSpecs = cols.toArray(new DataGenerator.ColSpec[0]); 			    
 			    
 			    // load in mapping files
 			    for(int i=0; i<dg.colSpecs.length; i++) {
 			    	DataGenerator.ColSpec col = dg.colSpecs[i];
 			    	if (col.mapfile != null) { 			    		
 			    		reader = new BufferedReader(new InputStreamReader(fs.open(new Path(col.mapfile))));
 			    		Map<Integer, Object> map = dg.colSpecs[i].map;
 			    		while((line = reader.readLine()) != null) {
 					    	String[] fields = line.split("\t");
 					    	int key = Integer.parseInt(fields[0]);
 					    	if (col.datatype == DataGenerator.Datatype.DOUBLE) {
 					    		map.put(key, Double.parseDouble(fields[1]));
 					    	}else if (col.datatype == DataGenerator.Datatype.FLOAT) {
 					    		map.put(key, Float.parseFloat(fields[1]));
 					    	}else {
 					    		map.put(key, fields[1]);
 					    	}
 					    }
 			    		
 			    		reader.close();
 			    	}
 			    }
 		    }catch(IOException e) {
 		    	throw new RuntimeException("Failed to load config file. " + e);
 		    }
  		}
 		
 		public void map(LongWritable key, Text value, OutputCollector<String, String> output, Reporter reporter) throws IOException {
 			int intialsz = dg.colSpecs.length * 50;
 			
 			if (!hasInput) {
 				long numRows = Long.parseLong(value.toString().trim()); 		
 	 	        dg.numRows = numRows; 	 	         	       
 	 	        
 	 	        for (int i = 0; i < numRows; i++) {
 	 	        	StringWriter str = new StringWriter(intialsz);
 	 	        	PrintWriter pw = new PrintWriter(str);
 	 	        	dg.writeLine(pw); 	    
 	 	        	output.collect(null, str.toString()); 	        	 	        
 	 	        	
 	 	        	if ((i+1) % 10000 == 0) {
 	 	        		reporter.progress();
 	 	        		reporter.setStatus("" + (i+1) + " tuples generated.");  	        	
 	 	        	}
 	 	        }	       
 			} else {
 				StringWriter str = new StringWriter(intialsz);
	 	        PrintWriter pw = new PrintWriter(str);
	 	        pw.write(value.toString());
	 	        dg.writeLine(pw); 	    
	 	        output.collect(null, str.toString()); 	        	 	        	 	        	
 			}
 		}
 	}
}




