package org.apache.pig.impl.streaming;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.pig.builtin.PigStorage;


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
public class StreamingCommand implements Serializable {
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
    
    /**
     * Create a new <code>StreamingCommand</code> with the given command.
     * 
     * @param command streaming command to be executed
     * @param argv parsed arguments of the <code>command</code>
     */
    public StreamingCommand(String[] argv) {
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
    public void addPathToShip(String path) {
        shipSpec.add(path);
    }

    /**
     * Add a file to be cached on execute nodes on the cluster. The file is
     * assumed to be available at the shared filesystem.
     * 
     * @param path path of the file to be cached on the execute nodes
     */
    public void addPathToCache(String path) {
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
        return executable;
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
    implements Comparable<HandleSpec>, Serializable {
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
            this(handleName, PigStorage.class.getName());
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
            return name + " using " + spec;
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
    }
}