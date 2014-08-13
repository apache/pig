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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.hadoop.util.Shell;


/**
 * {@link StreamingCommand} represents the specification of an external
 * command to be executed in a Pig Query. 
 * 
 * <code>StreamingCommand</code> encapsulates all relevant details of the
 * command specified by the user either directly via the <code>STREAM</code>
 * operator or indirectly via a <code>DEFINE</code> operator. It includes
 * details such as input/output/error specifications and also files to be
 * shipped to the cluster and files to be cached.
 */
public class StreamingCommand implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    // External command to be executed and it's parsed components
    String executable;                   
    String[] argv;
    
    // Files to be shipped to the cluster in-order to be executed
    List<String> shipSpec = new LinkedList<String>();                  

    // Files to be shipped to the cluster in-order to be executed
    List<String> cacheSpec = new LinkedList<String>();                 

    /**
     * Handle to communicate with the external process.
     */
    public enum Handle {INPUT, OUTPUT}
    
    /**
     * Map from the the stdin/stdout/stderr handles to their specifications
     */
    Map<Handle, List<HandleSpec>> handleSpecs = 
        new TreeMap<Handle, List<HandleSpec>>();
    
    // Should the stderr of the process be persisted?
    boolean persistStderr = false;
    
    // Directory where the process's stderr logs should be persisted.
    String logDir;
    
    // Limit on the number of persisted log-files 
    int logFilesLimit = 100;
    public static final int MAX_TASKS = 100;
    
    boolean shipFiles = true;
    
    private PigContext pigContext;

    /**
     * Create a new <code>StreamingCommand</code> with the given command.
     * 
     * @param pigContext PigContext structure
     * @param argv parsed arguments of the <code>command</code>
     */
    public StreamingCommand(PigContext pigContext, String[] argv) {
        this.pigContext = pigContext;
        this.argv = argv;

        // Assume that argv[0] is the executable
        this.executable = this.argv[0];
    }
    
    /**
     * Get the command to be executed.
     * 
     * @return the command to be executed
     */
    public String getExecutable() {
        return executable;
    }
    
    /**
     * Set the executable for the <code>StreamingCommand</code>.
     * 
     * @param executable the executable for the <code>StreamingCommand</code>
     */
    public void setExecutable(String executable) {
        this.executable = executable;
    }
    
    /**
     * Set the command line arguments for the <code>StreamingCommand</code>.
     * 
     * @param argv the command line arguments for the 
     *             <code>StreamingCommand</code>
     */
    public void setCommandArgs(String[] argv) {
        this.argv = argv;
    }
    
    /**
     * Get the parsed command arguments.
     * 
     * @return the parsed command arguments as <code>String[]</code>
     */
    public String[] getCommandArgs() {
        return argv;
    }

    /**
     * Get the list of files which need to be shipped to the cluster.
     * 
     * @return the list of files which need to be shipped to the cluster
     */
    public List<String> getShipSpecs() {
        return shipSpec;
    }
    
    /**
     * Get the list of files which need to be cached on the execute nodes.
     * 
     * @return the list of files which need to be cached on the execute nodes
     */
    public List<String> getCacheSpecs() {
        return cacheSpec;
    }

    /**
     * Add a file to be shipped to the cluster. 
     * 
     * Users can use this to distribute executables and other necessary files
     * to the clusters.
     * 
     * @param path path of the file to be shipped to the cluster
     */
    public void addPathToShip(String path) throws IOException {
        // Validate
        File file = new File(path);
        if (!file.exists()) {
            throw new IOException("Invalid ship specification: '" + path + 
                                  "' does not exist!");
        } else if (file.isDirectory()) {
            throw new IOException("Invalid ship specification: '" + path + 
                                  "' is a directory and can't be shipped!");
        }
        shipSpec.add(path);
    }

    /**
     * Add a file to be cached on execute nodes on the cluster. The file is
     * assumed to be available at the shared filesystem.
     * 
     * @param path path of the file to be cached on the execute nodes
     */
    public void addPathToCache(String path) throws IOException {
        // Validate
        URI pathUri = null;
        URI dfsPath = null;
        try {
            // On Windows, replace "\" into "/"
            if (Shell.WINDOWS) {
                path = path.replaceAll("\\\\", "/");
            }
            pathUri = new URI(path);
            
            // Strip away the URI's _fragment_ and _query_
            dfsPath = new URI(pathUri.getScheme(), pathUri.getAuthority(), 
                              pathUri.getPath(), null, null);
        } catch (URISyntaxException urise) {
            throw new IOException("Invalid cache specification: " + path);
        }
        
        boolean exists = false;
        try {
            exists = FileLocalizer.fileExists(dfsPath.toString(), pigContext);
        } catch (IOException ioe) {
            // Throw a better error message...
            throw new IOException("Invalid cache specification: '" + dfsPath + 
                                  "' does not exist!");
        } 
        
        if (!exists) {
            throw new IOException("Invalid cache specification: '" + dfsPath + 
                                  "' does not exist!");
        } else if (FileLocalizer.isDirectory(dfsPath.toString(), pigContext)) {
            throw new IOException("Invalid cache specification: '" + dfsPath + 
                                  "' is a directory and can't be cached!");
        }

        cacheSpec.add(path);
    }

    /**
     * Attach a {@link HandleSpec} to a given {@link Handle}
     * @param handle <code>Handle</code> to which the specification is to 
     *               be attached.
     * @param handleSpec <code>HandleSpec</code> for the given handle.
     */
    public void addHandleSpec(Handle handle, HandleSpec handleSpec) {
        List<HandleSpec> handleSpecList = handleSpecs.get(handle);
        
        if (handleSpecList == null) {
            handleSpecList = new LinkedList<HandleSpec>();
            handleSpecs.put(handle, handleSpecList);
        }
        
        handleSpecList.add(handleSpec);
    }
    
    /**
     * Set the input specification for the <code>StreamingCommand</code>.
     * 
     * @param spec input specification
     */
    public void setInputSpec(HandleSpec spec) {
        List<HandleSpec> inputSpecs = getHandleSpecs(Handle.INPUT);
        if (inputSpecs == null || inputSpecs.size() == 0) {
            addHandleSpec(Handle.INPUT, spec);
        } else {
            inputSpecs.set(0, spec);
        }
    }
    
    /**
     * Get the input specification of the <code>StreamingCommand</code>.
     * 
     * @return input specification of the <code>StreamingCommand</code>
     */
    public HandleSpec getInputSpec() {
        List<HandleSpec> inputSpecs = getHandleSpecs(Handle.INPUT);
        if (inputSpecs == null || inputSpecs.size() == 0) {
            addHandleSpec(Handle.INPUT, new HandleSpec("stdin", PigStreaming.class.getName()));
        }
        return getHandleSpecs(Handle.INPUT).get(0);        
    }
    
    /**
     * Set the specification for the primary output of the 
     * <code>StreamingCommand</code>.
     * 
     * @param spec specification for the primary output of the 
     *             <code>StreamingCommand</code>
     */
    public void setOutputSpec(HandleSpec spec) {
        List<HandleSpec> outputSpecs = getHandleSpecs(Handle.OUTPUT);
        if (outputSpecs == null || outputSpecs.size() == 0) {
            addHandleSpec(Handle.OUTPUT, spec);
        } else {
            outputSpecs.set(0, spec);
        }
    }
    
    /**
     * Get the specification of the primary output of the 
     * <code>StreamingCommand</code>.
     * 
     * @return specification of the primary output of the 
     *         <code>StreamingCommand</code>
     */
    public HandleSpec getOutputSpec() {
        List<HandleSpec> outputSpecs = getHandleSpecs(Handle.OUTPUT);
        if (outputSpecs == null || outputSpecs.size() == 0) {
            addHandleSpec(Handle.OUTPUT, new HandleSpec("stdout", PigStreaming.class.getName()));
        }
        return getHandleSpecs(Handle.OUTPUT).get(0);
    }
    
    /**
     * Get specifications for the given <code>Handle</code>.
     * 
     * @param handle <code>Handle</code> of the stream
     * @return specification for the given <code>Handle</code>
     */
    public List<HandleSpec> getHandleSpecs(Handle handle) {
        return handleSpecs.get(handle);
    }
    
    /**
     * Should the stderr of the managed process be persisted?
     * 
     * @return <code>true</code> if the stderr of the managed process should be
     *         persisted, <code>false</code> otherwise.
     */
    public boolean getPersistStderr() {
        return persistStderr;
    }

    /**
     * Specify if the stderr of the managed process should be persisted.
     * 
     * @param persistStderr <code>true</code> if the stderr of the managed 
     *                      process should be persisted, else <code>false</code>
     */
    public void setPersistStderr(boolean persistStderr) {
        this.persistStderr = persistStderr;
    }

    /**
     * Get the directory where the log-files of the command are persisted.
     * 
     * @return the directory where the log-files of the command are persisted
     */
    public String getLogDir() {
        return logDir;
    }

    /**
     * Set the directory where the log-files of the command are persisted.
     * 
     * @param logDir the directory where the log-files of the command are persisted
     */
    public void setLogDir(String logDir) {
        this.logDir = logDir;
        if (this.logDir.startsWith("/")) {
            this.logDir = this.logDir.substring(1);
        }
        setPersistStderr(true);
    }

    /**
     * Get the maximum number of tasks whose stderr logs files are persisted.
     * 
     * @return the maximum number of tasks whose stderr logs files are persisted
     */
    public int getLogFilesLimit() {
        return logFilesLimit;
    }

    /**
     * Set the maximum number of tasks whose stderr logs files are persisted.
     * @param logFilesLimit the maximum number of tasks whose stderr logs files 
     *                      are persisted
     */
    public void setLogFilesLimit(int logFilesLimit) {
        this.logFilesLimit = Math.min(MAX_TASKS, logFilesLimit);
    }

    /**
     * Set whether files should be shipped or not.
     * 
     * @param shipFiles <code>true</code> if files of this command should be
     *                  shipped, <code>false</code> otherwise
     */
    public void setShipFiles(boolean shipFiles) {
        this.shipFiles = shipFiles;
    }

    /**
     * Get whether files for this command should be shipped or not.
     * 
     * @return <code>true</code> if files of this command should be shipped, 
     *         <code>false</code> otherwise
     */
    public boolean getShipFiles() {
        return shipFiles;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (String arg : getCommandArgs()) {
            sb.append(arg);
            sb.append(" ");
        }
        sb.append("(" + getInputSpec().toString() + "/"+getOutputSpec() + ")");

        return sb.toString();
    }
    
    public Object clone() {
      try {
        StreamingCommand clone = (StreamingCommand)super.clone();

        clone.shipSpec = new ArrayList<String>(shipSpec);
        clone.cacheSpec = new ArrayList<String>(cacheSpec);
        
        clone.handleSpecs = new HashMap<Handle, List<HandleSpec>>();
        for (Map.Entry<Handle, List<HandleSpec>> e : handleSpecs.entrySet()) {
          List<HandleSpec> values = new ArrayList<HandleSpec>();
          for (HandleSpec spec : e.getValue()) {
            values.add((HandleSpec)spec.clone());
          }
          clone.handleSpecs.put(e.getKey(), values);
        }

        return clone;
      } catch (CloneNotSupportedException cnse) {
        // Shouldn't happen since we do implement Clonable
        throw new InternalError(cnse.toString());
      }
    }


    /**
     * Specification about the usage of the {@link Handle} to communicate
     * with the external process.
     * 
     * It specifies the stream-handle which can be one of <code>stdin</code>/
     * <code>stdout</code>/<code>stderr</code> or a named file and also the
     * serializer/deserializer specification to be used to read/write data 
     * to/from the stream.
     */
    public static class HandleSpec 
    implements Comparable<HandleSpec>, Serializable, Cloneable {
        private static final long serialVersionUID = 1L;

        String name;
        String spec;
        
        /**
         * Create a new {@link HandleSpec} with a given name using the default
         * {@link PigStorage} serializer/deserializer.
         * 
         * @param handleName name of the handle (one of <code>stdin</code>,
         *                   <code>stdout</code> or a file-path)
         */
        public HandleSpec(String handleName) {
            this(handleName, PigStreaming.class.getName());
        }
        
        /**
         * Create a new {@link HandleSpec} with a given name using the default
         * {@link PigStorage} serializer/deserializer.
         * 
         * @param handleName name of the handle (one of <code>stdin</code>,
         *                   <code>stdout</code> or a file-path)
         * @param spec serializer/deserializer spec
         */
        public HandleSpec(String handleName, String spec) {
            this.name = handleName;
            this.spec = spec;
        }

        public int compareTo(HandleSpec o) {
            return this.name.compareTo(o.name);
        }
        
        public String toString() {
            return name + "-" + spec;
        }

        /**
         * Get the <b>name</b> of the <code>HandleSpec</code>.
         * 
         * @return the <b>name</b> of the <code>HandleSpec</code> (one of 
         *         <code>stdin</code>, <code>stdout</code> or a file-path)
         */
        public String getName() {
            return name;
        }

        /**
         * Set the <b>name</b> of the <code>HandleSpec</code>.
         * 
         * @param name <b>name</b> of the <code>HandleSpec</code> (one of 
         *         <code>stdin</code>, <code>stdout</code> or a file-path)
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * Get the serializer/deserializer spec of the <code>HandleSpec</code>.
         * 
         * @return the serializer/deserializer spec of the 
         *         <code>HandleSpec</code>
         */
        public String getSpec() {
            return spec;
        }

        /**
         * Set the serializer/deserializer spec of the <code>HandleSpec</code>.
         * 
         * @param spec the serializer/deserializer spec of the 
         *             <code>HandleSpec</code>
         */
        public void setSpec(String spec) {
            this.spec = spec;
        }
        
        public boolean equals(Object obj) {
            if (obj instanceof HandleSpec){
                HandleSpec other = (HandleSpec)obj;
                return (other != null && name.equals(other.name) && spec.equals(other.spec));
            } else 
                return false;
        }

        public int hashCode() {
            return name.hashCode();
        }

        public Object clone() {
          try {
            return super.clone();
          } catch (CloneNotSupportedException cnse) {
            // Shouldn't happen since we do implement Clonable
            throw new InternalError(cnse.toString());
          }
        }
    }
}
