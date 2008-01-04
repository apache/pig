package org.apache.pig.tools.grunt;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigscript.parser.PigScriptParser;
import org.apache.pig.tools.pigscript.parser.PigScriptParserTokenManager;

public class GruntParser extends PigScriptParser {


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
				System.err.println(e.getMessage());
			}
	}

	public void setParams(PigServer pigServer)
	{
		mPigServer = pigServer;
		mDfs = mPigServer.getPigContext().getDfs();
		mConf = mPigServer.getPigContext().getConf();
		mJobClient = mPigServer.getPigContext().getJobClient();
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
			else
				throw new ParseException("Invalid value " + value + " provided for " + key);
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
		byte buffer[] = new byte[65536];
		Path dfsPath = new Path(path);
		int rc;
		
		if (!mDfs.exists(dfsPath))
			throw new IOException("Directory " + path + " does not exist.");

		if (mDfs.getFileStatus(dfsPath).isDir()) 
		{
			FileStatus fileStat[] = mDfs.listStatus(dfsPath);
			for (int j = 0; j < fileStat.length; j++)
			{
				Path curPath = fileStat[j].getPath();
				if (!mDfs.isFile(curPath)) continue;
				FSDataInputStream is = mDfs.open(curPath);
				while ((rc = is.read(buffer)) > 0)
					System.out.write(buffer, 0, rc);
				is.close();
			}
		} 
		else 
		{
			FSDataInputStream is = mDfs.open(dfsPath);
			while ((rc = is.read(buffer)) > 0)
				System.out.write(buffer, 0, rc);
			is.close();
		}
	}

	protected void processCD(String path) throws IOException
	{	
		if (path == null)
			mDfs.setWorkingDirectory(new Path("/user/" + System.getProperty("user.name")));
		else
		{
			Path dfsDir = new Path(path);

			if (!mDfs.exists(dfsDir))
				throw new IOException("Directory " + path + " does not exist.");

			if (!mDfs.getFileStatus(dfsDir).isDir())
				throw new IOException(path + " is not a directory.");

			mDfs.setWorkingDirectory(dfsDir);
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
		RunningJob job = mJobClient.getJob(jobid);
		if (job == null)
			System.out.println("Job with id " + jobid + " is not active");
		else
		{	
			job.killJob();
			System.err.println("kill submited.");
		}
	}

	protected void processLS(String path) throws IOException
	{
		Path dir;
		if(path == null)
                	dir = mDfs.getWorkingDirectory();
		else
		{
			dir = new Path(path);
			if (!mDfs.exists(dir))
				throw new IOException("File or directory " + path + " does not exist.");
		}

		FileStatus fileStat[] = mDfs.listStatus(dir);
		for (int j = 0; j < fileStat.length; j++)
		{
            if (fileStat[j].isDir())
           		System.out.println(fileStat[j].getPath() + "\t<dir>");
			else
				System.out.println(fileStat[j].getPath() + "<r " + fileStat[j].getReplication() + ">\t" + fileStat[j].getLen());
                }
	}
	
	protected void processPWD() throws IOException 
	{
		System.out.println(mDfs.getWorkingDirectory());
	}

	protected void printHelp() 
	{
		System.err.println("Commands:");
		System.err.println("<pig latin statement>;");
		System.err.println("store <alias> into <filename> [using <functionSpec>]");
		System.err.println("dump <alias>");
		System.err.println("describe <alias>");
		System.err.println("kill <job_id>");
		System.err.println("ls <path>\r\ndu <path>\r\nmv <src> <dst>\r\ncp <src> <dst>\r\nrm <src>");
		System.err.println("copyFromLocal <localsrc> <dst>\r\ncd <dir>\r\npwd");
		System.err.println("cat <src>\r\ncopyToLocal <src> <localdst>\r\nmkdir <path>");
		System.err.println("cd <path>");
		System.err.println("define <functionAlias> <functionSpec>");
		System.err.println("register <udfJar>");
		System.err.println("set key value");
		System.err.println("quit");
	}

	protected void processMove(String src, String dst) throws IOException
	{
		Path srcPath = new Path(src);
		Path dstPath = new Path(dst);
		if (!mDfs.exists(srcPath))
			throw new IOException("File or directory " + src + " does not exist.");

		{mDfs.rename(srcPath, dstPath);}
	}
	
	protected void processCopy(String src, String dst) throws IOException
	{
        FileUtil.copy(mDfs, new Path(src), mDfs, new Path(dst), false, mConf);
	}
	
	protected void processCopyToLocal(String src, String dst) throws IOException
	{
		mDfs.copyToLocalFile(new Path(src), new Path(dst));
	}

	protected void processCopyFromLocal(String src, String dst) throws IOException
	{
        mDfs.copyFromLocalFile(new Path(src), new Path(dst));
	}
	
	protected void processMkdir(String dir) throws IOException
	{
		mDfs.mkdirs(new Path(dir));
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
		Path dfsPath = new Path(path);
		if (!mDfs.exists(dfsPath))
			throw new IOException("File or directory " + path + " does not exist.");

		mDfs.delete(dfsPath);
	}

	private PigServer mPigServer;
	private FileSystem mDfs;
	private Configuration mConf;
	private JobClient mJobClient;
	private boolean mDone;

}
