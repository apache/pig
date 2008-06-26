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
package org.apache.pig.impl.streaming;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.streaming.InputHandler.InputType;
import org.apache.pig.impl.streaming.OutputHandler.OutputType;

/**
 * {@link ExecutableManager} manages an external executable which processes data
 * in a Pig query.
 * 
 * The <code>ExecutableManager</code> is responsible for startup/teardown of the 
 * external process and also for managing it.
 * It feeds input records to the executable via it's <code>stdin</code>, 
 * collects the output records from the <code>stdout</code> and also diagnostic 
 * information from the <code>stdout</code>.
 */
public class ExecutableManager {
	private static final Log LOG = 
		LogFactory.getLog(ExecutableManager.class.getName());
    private static final int SUCCESS = 0;
    private static final String PATH = "PATH";
    private static final String BASH = "bash";
    
	protected StreamingCommand command;        // Streaming command to be run
	String[] argv;                             // Parsed/split commands

	Process process;                           // Handle to the process
    protected int exitCode = -127;             // Exit code of the process
	
	protected DataOutputStream stdin;          // stdin of the process
	
	ProcessOutputThread stdoutThread;          // thread to get process output
	InputStream stdout;                        // stdout of the process
	                                           // interpret the process' output
	
	ProcessErrorThread stderrThread;           // thread to get process output
	InputStream stderr;                        // stderr of the process
	
	DataCollector endOfPipe;

	// Input/Output handlers
	InputHandler inputHandler;
	OutputHandler outputHandler;

	Properties properties;

	// Statistics
	protected long inputRecords = 0;
	protected long inputBytes = 0;
	protected long outputRecords = 0;
	protected long outputBytes = 0;
	
	protected volatile Throwable outerrThreadsError;

	/**
	 * Create a new {@link ExecutableManager}.
	 */
	public ExecutableManager() {}
	
	/**
	 * Configure and initialize the {@link ExecutableManager}.
	 * 
	 * @param properties {@link Properties} for the 
	 *                   <code>ExecutableManager</code>
	 * @param command {@link StreamingCommand} to be run by the 
	 *                <code>ExecutableManager</code>
	 * @param endOfPipe {@link DataCollector} to be used to push results of the
	 *                  <code>StreamingCommand</code> down
	 * @throws IOException
	 * @throws ExecException
	 */
	public void configure(Properties properties, StreamingCommand command, 
	                      DataCollector endOfPipe) 
	throws IOException, ExecException {
	    this.properties = properties;
	    
		this.command = command;
		this.argv = this.command.getCommandArgs();

		// Create the input/output handlers
		this.inputHandler = HandlerFactory.createInputHandler(command);
		this.outputHandler = 
		    HandlerFactory.createOutputHandler(command);
		
		// Successor
		this.endOfPipe = endOfPipe;
	}
	
	/**
	 * Close and cleanup the {@link ExecutableManager}.
	 * 
	 * @throws IOException
	 * @throws ExecException
	 */
	public void close() throws IOException, ExecException {
	    try {
	        // Close the InputHandler, which in some cases lets the process
	        // terminate
	        inputHandler.close();

	        // Check if we need to start the process now ...
	        if (inputHandler.getInputType() == InputType.ASYNCHRONOUS) {
	            exec();
	        }

	        // Wait for the process to exit 
	        try {
	            exitCode = process.waitFor();
	        } catch (InterruptedException ie) {}

	        // Wait for stdout thread to complete
	        try {
	            if (stdoutThread != null) {
	                stdoutThread.join(0);
	            }
	            stdoutThread = null;
	        } catch (InterruptedException ie) {}
	        
            // Wait for stderr thread to complete
	        try {
	            if (stderrThread != null) {
	                stderrThread.join(0);
	            }
	            stderrThread = null;
	        } catch (InterruptedException ie) {}

	        // Clean up the process
	        process.destroy();
	        process = null;

	        LOG.debug("Process exited with: " + exitCode);
	        if (exitCode != SUCCESS) {
	            throw new ExecException(command + " failed with exit status: " + 
	                    exitCode);
	        }

	        if (outputHandler.getOutputType() == OutputType.ASYNCHRONOUS) {
	            // Trigger the outputHandler
	            outputHandler.bindTo("", null, 0, -1);

	            // Start the thread to process the output and wait for
	            // it to terminate
	            stdoutThread = new ProcessOutputThread(outputHandler);
	            stdoutThread.start();

	            try {
	                stdoutThread.join(0);
	            } catch (InterruptedException ie) {}
	        }
	    } finally {
	        // Cleanup, release resources ...
	        if (process != null) {
              // Get the exit code 
              try {
                  exitCode = process.waitFor();
              } catch (InterruptedException ie) {}

              // Cleanup the process 
              process.destroy();
	        }

	        if (stdoutThread != null) {
	            stdoutThread.interrupt();
	        }
	    
	        if (stderrThread != null) {
	            stderrThread.interrupt();
	        }
	    }
	    
	    // Check if there was a problem with the managed process
	    if (outerrThreadsError != null) {
	        throw new IOException("Output/Error thread failed with: " +
	                              outerrThreadsError);
	    }
 
	}

	/**
	 * Convert path from Windows convention to Unix convention. Invoked under cygwin. 
	 * 
	 * @param path path in Windows convention
	 * @return path in Unix convention, null if fail
	 */
    private String parseCygPath(String path)
    {
        String[] command = new String[] {"cygpath", "-u", path};
        Process p=null;
        try {
            p = Runtime.getRuntime().exec(command);
        }
        catch (IOException e)
        {
            return null;
        }
        int exitVal=0;
        try {
            exitVal = p.waitFor();
        }
        catch (InterruptedException e)
        {
            return null;
        }
        if (exitVal!=0)
        	return null;
        String line=null;
        try {
            InputStreamReader isr = new InputStreamReader(p.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            line = br.readLine();
        }
        catch (IOException e)
        {
            return null;
        }
        return line;
    }

	/**
	 * Set up the run-time environment of the managed process.
	 * 
	 * @param pb {@link ProcessBuilder} used to exec the process
	 */
	protected void setupEnvironment(ProcessBuilder pb) {
	    String separator = ":";
	    Map<String, String> env = pb.environment();
	    
	    // Add the current-working-directory to the $PATH
	    File dir = pb.directory();
	    String cwd = (dir != null) ? 
	            dir.getAbsolutePath() : System.getProperty("user.dir");
	    
	    if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
	    {
	    	String unixCwd = parseCygPath(cwd);
	    	if (unixCwd==null)
	    		throw new RuntimeException("Can not convert Windows path to Unix path under cygwin");
	    	cwd = unixCwd;
	    }
	    	
	    String envPath = env.get(PATH);
	    if (envPath == null) {
	        envPath = cwd;
	    } else {
	        envPath = envPath + separator + cwd;
	    }
	    env.put(PATH, envPath);
	}
	
	/**
	 * Start execution of the external process.
	 * 
	 * This takes care of setting up the environment of the process and also
	 * starts {@link ProcessErrorThread} to process the <code>stderr</code> of
	 * the managed process.
	 * 
	 * @throws IOException
	 */
	protected void exec() throws IOException {
	    // Set the actual command to run with 'bash -c exec ...'
        List<String> cmdArgs = new ArrayList<String>();
        cmdArgs.add(BASH);
        cmdArgs.add("-c");
        StringBuffer sb = new StringBuffer();
        sb.append("exec ");
	    for (String arg : argv) {
            sb.append(arg);
            sb.append(" ");
	    }
	    cmdArgs.add(sb.toString());
	    
        // Start the external process
        ProcessBuilder processBuilder = 
            new ProcessBuilder(cmdArgs.toArray(new String[cmdArgs.size()]));
        setupEnvironment(processBuilder);
        process = processBuilder.start();
        LOG.debug("Started the process for command: " + command);
        
        // Pick up the process' stderr stream and start the thread to 
        // process the stderr stream
        stderr = 
            new DataInputStream(new BufferedInputStream(process.getErrorStream()));
        stderrThread = new ProcessErrorThread();
        stderrThread.start();

        // Check if we need to handle the process' stdout directly
        if (outputHandler.getOutputType() == OutputType.SYNCHRONOUS) {
            // Get hold of the stdout of the process
            stdout = 
                new DataInputStream(new BufferedInputStream(process.getInputStream()));
            
            // Bind the stdout to the OutputHandler
            outputHandler.bindTo("", new BufferedPositionedInputStream(stdout), 
                                 0, Long.MAX_VALUE);
            
            // Start the thread to process the executable's stdout
            stdoutThread = new ProcessOutputThread(outputHandler);
            stdoutThread.start();
        }
	}
	
	/**
	 * Start execution of the {@link ExecutableManager}.
	 * 
	 * @throws IOException
	 */
	public void run() throws IOException {
	    // Check if we need to exec the process NOW ...
	    if (inputHandler.getInputType() == InputType.ASYNCHRONOUS) {
	        return;
	    }
	    
		// Start the executable ...
	    exec();
        stdin = 
            new DataOutputStream(new BufferedOutputStream(process.getOutputStream()));
	    inputHandler.bindTo(stdin);
	}

	/**
	 * Send the given {@link Datum} to the external command managed by the
	 * {@link ExecutableManager}.
	 * 
	 * @param d <code>Datum</code> to be sent to the external command
	 * @throws IOException
	 */
	public void add(Datum d) throws IOException {
        // Check if there was a problem with the managed process
	    if (outerrThreadsError != null) {
	        throw new IOException("Output/Error thread failed with: " +
	                              outerrThreadsError);
	    }
	    
		// Pass the serialized tuple to the executable via the InputHandler
	    Tuple t = (Tuple)d;
	    inputHandler.putNext(t);
	    inputBytes += t.getMemorySize();
	    inputRecords++;
	}

	/**
	 * Workhorse to process the output of the managed process.
	 * 
	 * The <code>ExecutableManager</code>, by default, just pushes the received
	 * <code>Datum</code> into eval-pipeline to be processed by the successor.
	 * 
	 * @param d <code>Datum</code> to process
	 */
	protected void processOutput(Datum d) {
		endOfPipe.add(d);
		outputRecords++;
	}
	
	class ProcessOutputThread extends Thread {

	    OutputHandler outputHandler;

		ProcessOutputThread(OutputHandler outputHandler) {
			setDaemon(true);
			this.outputHandler = outputHandler;
		}

		public void run() {
			try {
				// Read tuples from the executable and push them down the pipe
				Tuple tuple = null;
				while ((tuple = outputHandler.getNext()) != null) {
					processOutput(tuple);
					outputBytes += tuple.getMemorySize();
				}

				outputHandler.close();
			} catch (Throwable t) {
			    // Note that an error occurred 
			    outerrThreadsError = t;

				LOG.warn(t);
				try {
				    outputHandler.close();
				} catch (IOException ioe) {
					LOG.info(ioe);
				}
				throw new RuntimeException(t);
			}
		}
	}

	/**
	 * Workhorse to process the stderr stream of the managed process.
	 * 
	 * By default <code>ExecuatbleManager</code> just sends out the received
	 * error message to the <code>stderr</code> of itself.
	 * 
	 * @param error error message from the managed process.
	 */
	protected void processError(String error) {
		// Just send it out to our stderr
		System.err.print(error);
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
					processError(error+"\n");
				}

				if (stderr != null) {
					stderr.close();
					LOG.debug("ProcessErrorThread done");
				}
			} catch (Throwable t) {
			    // Note that an error occurred 
			    outerrThreadsError = t;

				LOG.warn(t);
				try {
					if (stderr != null) {
						stderr.close();
					}
				} catch (IOException ioe) {
					LOG.info(ioe);
				}
                throw new RuntimeException(t);
			}
		}
	}
}
