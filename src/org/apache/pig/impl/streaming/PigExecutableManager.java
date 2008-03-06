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
package org.apache.pig.backend.streaming;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.parser.ParseException;

/**
 * {@link PigExecutableManager} manages an external executable which processes data
 * in a Pig query.
 * 
 * The <code>PigExecutableManager</code> is responsible for startup/teardown of the 
 * external process and also for managing it.
 * It feeds input records to the executable via it's <code>stdin</code>, 
 * collects the output records from the <code>stdout</code> and also diagnostic 
 * information from the <code>stdout</code>.
 */
public class PigExecutableManager {
	private static final Log LOG = 
		LogFactory.getLog(PigExecutableManager.class.getName());

	String command;                            // Streaming command to be run
	String[] argv;                             // Parsed/split commands 

	Process process;                           // Handle to the process
	private static int SUCCESS = 0;
	
	protected DataOutputStream stdin;          // stdin of the process
	StoreFunc serializer;                      // serializer to be used to
	                                           // send data to the process
	
	ProcessOutputThread stdoutThread;          // thread to get process output
	InputStream stdout;                        // stdout of the process
	LoadFunc deserializer;                     // deserializer to be used to
	                                           // interpret the process' output
	
	ProcessErrorThread stderrThread;           // thread to get process output
	InputStream stderr;                        // stderr of the process
	
	DataCollector endOfPipe;

	public PigExecutableManager(String command, 
			                    LoadFunc deserializer, StoreFunc serializer, 
			                    DataCollector endOfPipe) throws Exception {
		this.command = command;
		
		this.argv = splitArgs(this.command);
		if (LOG.isDebugEnabled()) {
		    for (String cmd : argv) {
		        LOG.debug("argv: " + cmd);
		    }
		}
		
		this.deserializer = deserializer;
		this.serializer = serializer;
		this.endOfPipe = endOfPipe;
	}

	private static final char SINGLE_QUOTE = '\'';
	private static final char DOUBLE_QUOTE = '"';
	private static String[] splitArgs(String command) throws Exception {
		List<String> argv = new ArrayList<String>();

		int beginIndex = 0;
		
		while (beginIndex < command.length()) {
			// Skip spaces
		    while (Character.isWhitespace(command.charAt(beginIndex))) {
		        ++beginIndex;
		    }
			
			char delim = ' ';
			char charAtIndex = command.charAt(beginIndex);
			if (charAtIndex == SINGLE_QUOTE || charAtIndex == DOUBLE_QUOTE) {
				delim = charAtIndex;
			}
			
			int endIndex = command.indexOf(delim, beginIndex+1);
			if (endIndex == -1) {
				if (Character.isWhitespace(delim)) {
					// Reached end of command-line
					argv.add(command.substring(beginIndex));
					break;
				} else {
					// Didn't find the ending quote/double-quote
					throw new ParseException("Illegal command: " + command);
				}
			}
			
			if (Character.isWhitespace(delim)) {
				// Do not consume the space
				argv.add(command.substring(beginIndex, endIndex));
			} else {
				// Do not consume the quotes
				argv.add(command.substring(beginIndex+1, endIndex));
			}
			
			beginIndex = endIndex + 1;
		}
		
		return argv.toArray(new String[0]);
	}

	public void configure() {
	}

	public void close() throws Exception {
	    // Close the stdin to let the process terminate
		stdin.flush();
		stdin.close();
		stdin = null;
		
		// Wait for the process to exit and the stdout/stderr threads to complete
		int exitCode = -1;
		try {
			exitCode = process.waitFor();
			
			if (stdoutThread != null) {
				stdoutThread.join(0);
			}
			if (stderrThread != null) {
				stderrThread.join(0);
			}

		} catch (InterruptedException ie) {}

		// Clean up the process
		process.destroy();
		
        LOG.debug("Process exited with: " + exitCode);
        if (exitCode != SUCCESS) {
            throw new ExecException(command + " failed with exit status: " + 
                                       exitCode);
        }
	}

	public void run() throws IOException {
		// Run the executable
		ProcessBuilder processBuilder = new ProcessBuilder(argv);
		process = processBuilder.start();
		LOG.debug("Started the process for command: " + command);

		// Pick up the process' stdin/stdout/stderr streams
		stdin = 
			new DataOutputStream(new BufferedOutputStream(process.getOutputStream()));
		stdout = 
			new DataInputStream(new BufferedInputStream(process.getInputStream()));
		stderr = 
			new DataInputStream(new BufferedInputStream(process.getErrorStream()));

		// Attach the serializer to the stdin of the process for sending tuples 
		serializer.bindTo(stdin);
		
		// Attach the deserializer to the stdout of the process to get tuples
		deserializer.bindTo("", new BufferedPositionedInputStream(stdout), 0, 
				            Long.MAX_VALUE);
		
		// Start the threads to process the executable's stdout and stderr
		stdoutThread = new ProcessOutputThread(deserializer);
		stdoutThread.start();
		stderrThread = new ProcessErrorThread();
		stderrThread.start();
	}

	public void add(Datum d) throws IOException {
		// Pass the serialized tuple to the executable
		serializer.putNext((Tuple)d);
		stdin.flush();
	}

	/**
	 * Workhorse to process the stdout of the managed process.
	 * 
	 * The <code>PigExecutableManager</code>, by default, just pushes the received
	 * <code>Datum</code> into eval-pipeline to be processed by the successor.
	 * 
	 * @param d <code>Datum</code> to process
	 */
	protected void processOutput(Datum d) {
		endOfPipe.add(d);
	}
	
	class ProcessOutputThread extends Thread {

		LoadFunc deserializer;

		ProcessOutputThread(LoadFunc deserializer) {
			setDaemon(true);
			this.deserializer = deserializer;
		}

		public void run() {
			try {
				// Read tuples from the executable and push them down the pipe
				Tuple tuple = null;
				while ((tuple = deserializer.getNext()) != null) {
					processOutput(tuple);
				}

				if (stdout != null) {
					stdout.close();
					LOG.debug("ProcessOutputThread done");
				}
			} catch (Throwable th) {
				LOG.warn(th);
				try {
					if (stdout != null) {
						stdout.close();
					}
				} catch (IOException ioe) {
					LOG.info(ioe);
				}
				throw new RuntimeException(th);
			}
		}
	}

	/**
	 * Workhorse to process the stderr stream of the managed process.
	 * 
	 * By default <code>PigExecuatbleManager</code> just sends out the received
	 * error message to the <code>stderr</code> of itself.
	 * 
	 * @param error error message from the managed process.
	 */
	protected void processError(String error) {
		// Just send it out to our stderr
		System.err.println(error);
	}
	
	class ProcessErrorThread extends Thread {

		public ProcessErrorThread() {
			setDaemon(true);
		}

		public void run() {
			try {
				String error;
				BufferedReader reader = 
					new BufferedReader(new InputStreamReader(stderr));
				while ((error = reader.readLine()) != null) {
					processError(error);
				}

				if (stderr != null) {
					stderr.close();
					LOG.debug("ProcessErrorThread done");
				}
			} catch (Throwable th) {
				LOG.warn(th);
				try {
					if (stderr != null) {
						stderr.close();
					}
				} catch (IOException ioe) {
					LOG.info(ioe);
	                throw new RuntimeException(th);
				}
			}
		}
	}
}
