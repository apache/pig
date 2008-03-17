package org.apache.pig.tools.grunt;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.pig.PigServer;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigscript.parser.PigScriptParser;
import org.apache.pig.tools.pigscript.parser.PigScriptParserTokenManager;

public class GruntParser extends PigScriptParser {

    private final Log log = LogFactory.getLog(getClass());

    public GruntParser(Reader stream) {
        super(stream);
        init();
    }

    public GruntParser(InputStream stream, String encoding) {
        super(stream, encoding);
        init();
    }

    public GruntParser(InputStream stream) {
        super(stream);
        init();
    }

    public GruntParser(PigScriptParserTokenManager tm) {
        super(tm);
        init();
    }

    private void init() {
        // nothing, for now.
    }
    
    public void parseStopOnError() throws IOException, ParseException
    {
        prompt();
        mDone = false;
        while(!mDone)
            parse();
    }

    public void parseContOnError()
    {
        prompt();
        mDone = false;
        while(!mDone)
            try
            {
                parse();
            }
            catch(Exception e)
            {
                log.error(e.getMessage());
            }
    }

    public void setParams(PigServer pigServer)
    {
        mPigServer = pigServer;
        
        mDfs = mPigServer.getPigContext().getDfs();
        mLfs = mPigServer.getPigContext().getLfs();
        mConf = mPigServer.getPigContext().getConf();
        
        // TODO: this violates the abstraction layer decoupling between
        // front end and back end and needs to be changed.
        // Right now I am not clear on how the Job Id comes from to tell
        // the back end to kill a given job (mJobClient is used only in 
        // processKill)
        //
        ExecutionEngine execEngine = mPigServer.getPigContext().getExecutionEngine();
        if (execEngine instanceof HExecutionEngine) {
            mJobClient = ((HExecutionEngine)execEngine).getJobClient();
        }
        else {
            mJobClient = null;
        }
    }

    public void prompt()
    {
        if (mInteractive)
        {
            System.err.print("grunt> ");
            System.err.flush();
        }
    }
    
    protected void quit()
    {
        mDone = true;
    }
    
    protected void processRegisterFunc(String name, String expr) {
        mPigServer.registerFunction(name, expr);
    }
    
    protected void processDescribe(String alias) throws IOException {
        mPigServer.dumpSchema(alias);
    }

    protected void processExplain(String alias) throws IOException {
        mPigServer.explain(alias, System.out);
    }
    
    protected void processRegister(String jar) throws IOException {
        mPigServer.registerJar(jar);
    }

    protected void processSet(String key, String value) throws IOException, ParseException {
        if (key.equals("debug"))
        {
            if (value.equals("on") || value.equals("'on'"))
                mPigServer.debugOn();
            else if (value.equals("off") || value.equals("'off'"))
                mPigServer.debugOff();
            else {
                StringBuilder sb = new StringBuilder();
                sb.append("Invalid value ");
                sb.append(value);
                sb.append(" provided for ");
                sb.append(key);
                throw new ParseException(sb.toString());
            }
        }
        else if (key.equals("job.name"))
        {
            //mPigServer.setJobName(unquote(value));
            mPigServer.setJobName(value);
        }
        else
        {
            // other key-value pairs can go there
            // for now just throw exception since we don't support
            // anything else
            throw new ParseException("Unrecognized set key: " + key);
        }
    }
    
    protected void processStore(String alias, String file, String func) throws IOException {
        mPigServer.store(alias, file, func);
    }
        
    protected void processCat(String path) throws IOException
    {
        try {
            byte buffer[] = new byte[65536];
            ElementDescriptor dfsPath = mDfs.asElement(path);
            int rc;
            
            if (!dfsPath.exists()) {
                StringBuilder sb = new StringBuilder();
                sb.append("Directory ");
                sb.append(path);
                sb.append(" does not exist.");
                throw new IOException(sb.toString());
            }
    
            if (mDfs.isContainer(path)) {
                ContainerDescriptor dfsDir = (ContainerDescriptor) dfsPath;
                Iterator<ElementDescriptor> paths = dfsDir.iterator();
                
                while (paths.hasNext()) {
                    ElementDescriptor curElem = paths.next();
                    
                    if (mDfs.isContainer(curElem.toString())) {
                        continue;
                    }
                    
                    InputStream is = curElem.open();
                    while ((rc = is.read(buffer)) > 0) {
                        System.out.write(buffer, 0, rc);
                    }
                    is.close();                
                }
            }
            else {
                InputStream is = dfsPath.open();
                while ((rc = is.read(buffer)) > 0) {
                    System.out.write(buffer, 0, rc);
                }
                is.close();            
            }
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to Cat: " + path, e);
        }
    }

    protected void processCD(String path) throws IOException
    {    
        ContainerDescriptor container;

        try {
            if (path == null) {
                container = mDfs.asContainer("/user/" + System.getProperty("user.name"));
                mDfs.setActiveContainer(container);
            }
            else
            {
                container = mDfs.asContainer(path);
    
                if (!container.exists()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Directory ");
                    sb.append(path);
                    sb.append(" does not exist.");
                    throw new IOException(sb.toString());
                }
                
                if (!mDfs.isContainer(path)) {
                    throw new IOException(path + " is not a directory.");
                }
                
                mDfs.setActiveContainer(container);
            }
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to change working directory to " + 
                                  ((path == null) ? ("/user/" + System.getProperty("user.name")) 
                                                     : (path)), e);
        }
    }

    protected void processDump(String alias) throws IOException
    {
        Iterator result = mPigServer.openIterator(alias);
        while (result.hasNext())
        {
            Tuple t = (Tuple) result.next();
            System.out.println(t);
        }
    }

    protected void processKill(String jobid) throws IOException
    {
        if (mJobClient != null) {
            RunningJob job = mJobClient.getJob(jobid);
            if (job == null) {
                StringBuilder sb = new StringBuilder();
                sb.append("Job with id ");
                sb.append(jobid);
                sb.append(" is not active");
                System.out.println(sb.toString());
            } else {
                job.killJob();
                log.error("kill submited.");
            }
        }
    }
        
    protected void processLS(String path) throws IOException
    {
        try {
            ElementDescriptor pathDescriptor;
            
            if (path == null) {
                pathDescriptor = mDfs.getActiveContainer();
            }
            else {
                pathDescriptor = mDfs.asElement(path);
            }

            if (!pathDescriptor.exists()) {
                StringBuilder sb = new StringBuilder();
                sb.append("File or directory ");
                sb.append(path);
                sb.append(" does not exist.");
                throw new IOException(sb.toString());
            }
            
            if (mDfs.isContainer(pathDescriptor.toString())) {
                ContainerDescriptor container = (ContainerDescriptor) pathDescriptor;
                Iterator<ElementDescriptor> elems = container.iterator();
                
                while (elems.hasNext()) {
                    ElementDescriptor curElem = elems.next();
                    
                    if (mDfs.isContainer(curElem.toString())) {
                           System.out.println(curElem.toString() + "\t<dir>");
                    }
                    else {
                        Properties config = curElem.getConfiguration();
                        Map<String, Object> stats = curElem.getStatistics();
                        
                        String strReplication = config.getProperty(ElementDescriptor.BLOCK_REPLICATION_KEY);
                        String strLen = (String) stats.get(ElementDescriptor.LENGTH_KEY);

                        StringBuilder sb = new StringBuilder();
                        sb.append(curElem.toString());
                        sb.append("<r ");
                        sb.append(strReplication);
                        sb.append(">\t");
                        sb.append(strLen);
                        System.out.println(sb.toString());
                    }
                }
            }
            else {
                Properties config = pathDescriptor.getConfiguration();
                Map<String, Object> stats = pathDescriptor.getStatistics();

                String strReplication = (String) config.get(ElementDescriptor.BLOCK_REPLICATION_KEY);
                String strLen = (String) stats.get(ElementDescriptor.LENGTH_KEY);

                StringBuilder sb = new StringBuilder();
                sb.append(pathDescriptor.toString());
                sb.append("<r ");
                sb.append(strReplication);
                sb.append(">\t");
                sb.append(strLen);
                System.out.println(sb.toString());
            }
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to LS on " + path, e);
        }
    }
    
    protected void processPWD() throws IOException 
    {
        System.out.println(mDfs.getActiveContainer().toString());
    }

    protected void printHelp() 
    {
        System.out.println("Commands:");
        System.out.println("<pig latin statement>;");
        System.out.println("store <alias> into <filename> [using <functionSpec>]");
        System.out.println("dump <alias>");
        System.out.println("describe <alias>");
        System.out.println("kill <job_id>");
        System.out.println("ls <path>\r\ndu <path>\r\nmv <src> <dst>\r\ncp <src> <dst>\r\nrm <src>");
        System.out.println("copyFromLocal <localsrc> <dst>\r\ncd <dir>\r\npwd");
        System.out.println("cat <src>\r\ncopyToLocal <src> <localdst>\r\nmkdir <path>");
        System.out.println("cd <path>");
        System.out.println("define <functionAlias> <functionSpec>");
        System.out.println("register <udfJar>");
        System.out.println("set key value");
        System.out.println("quit");
    }

    protected void processMove(String src, String dst) throws IOException
    {
        try {
            ElementDescriptor srcPath = mDfs.asElement(src);
            ElementDescriptor dstPath = mDfs.asElement(dst);
            
            if (!srcPath.exists()) {
                StringBuilder sb = new StringBuilder();
                sb.append("File or directory ");
                sb.append(src);
                sb.append(" does not exist.");
                throw new IOException(sb.toString());
            }
            
            srcPath.rename(dstPath);
        }
        catch (DataStorageException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Failed to move ");
            sb.append(src);
            sb.append(" to ");
            sb.append(dst);
            throw WrappedIOException.wrap(sb.toString(), e);
        }
    }
    
    protected void processCopy(String src, String dst) throws IOException
    {
        try {
            ElementDescriptor srcPath = mDfs.asElement(src);
            ElementDescriptor dstPath = mDfs.asElement(dst);
            
            srcPath.copy(dstPath, mConf, false);
        }
        catch (DataStorageException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Failed to copy ");
            sb.append(src);
            sb.append(" to ");
            sb.append(dst);
            throw WrappedIOException.wrap(sb.toString(), e);
        }
    }
    
    protected void processCopyToLocal(String src, String dst) throws IOException
    {
        try {
            ElementDescriptor srcPath = mDfs.asElement(src);
            ElementDescriptor dstPath = mLfs.asElement(dst);
            
            srcPath.copy(dstPath, false);
        }
        catch (DataStorageException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Failed to copy ");
            sb.append(src);
            sb.append("to (locally) ");
            sb.append(dst);
            throw WrappedIOException.wrap(sb.toString(), e);
        }
    }

    protected void processCopyFromLocal(String src, String dst) throws IOException
    {
        try {
            ElementDescriptor srcPath = mLfs.asElement(src);
            ElementDescriptor dstPath = mDfs.asElement(dst);
            
            srcPath.copy(dstPath, false);
        }
        catch (DataStorageException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Failed to copy (loally) ");
            sb.append(src);
            sb.append(" to ");
            sb.append(dst);
            throw WrappedIOException.wrap(sb.toString(), e);
        }
    }
    
    protected void processMkdir(String dir) throws IOException
    {
        ContainerDescriptor dirDescriptor = mDfs.asContainer(dir);
        dirDescriptor.create();
    }
    
    protected void processPig(String cmd) throws IOException
    {
        if (cmd.charAt(cmd.length() - 1) != ';') 
            mPigServer.registerQuery(cmd + ";"); 
        else 
            mPigServer.registerQuery(cmd);
    }

    protected void processRemove(String path) throws IOException
    {
        ElementDescriptor dfsPath = mDfs.asElement(path);
        
        if (!dfsPath.exists()) {
            StringBuilder sb = new StringBuilder();
            sb.append("File or directory ");
            sb.append(path);
            sb.append(" does not exist.");
            throw new IOException(sb.toString());
        }
            
        dfsPath.delete();
    }

    private PigServer mPigServer;
    private DataStorage mDfs;
    private DataStorage mLfs;
    private Properties mConf;
    private JobClient mJobClient;
    private boolean mDone;

}
