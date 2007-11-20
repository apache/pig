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
package org.apache.pig.impl.io;

import java.lang.IllegalArgumentException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.Stack;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.mapreduceExec.PigInputFormat;
import org.apache.pig.impl.mapreduceExec.PigInputFormat.PigRecordReader;


public class FileLocalizer {
    static public final String LOCAL_PREFIX  = "file:";
    
    public static class DFSInputStreamIterator extends InputStream {
        InputStream current;
        Path        paths[];
        int         currentPath;
        FileSystem fs;
        
        public DFSInputStreamIterator(Path paths[], FileSystem fs) {
            this.paths = paths;
            this.fs = fs;
        }

        private boolean isEOF() throws IOException {
            if (current == null) {
                if (currentPath == paths.length)
                    return true;
                current = fs.open(paths[currentPath++]);
            }
            return false;
        }

        private void doNext() throws IOException {
            current.close();
            current = null;
        }

        @Override
		public int read() throws IOException {
            while (!isEOF()) {
                int rc = current.read();
                if (rc != -1)
                    return rc;
                doNext();
            }
            return -1;
        }

        @Override
		public int available() throws IOException {
            if (isEOF())
                return 0;
            return current.available();
        }

        @Override
		public void close() throws IOException {
            if (current != null) {
                current.close();
                current = null;
            }
            currentPath = paths.length;
        }

        @Override
		public int read(byte[] b, int off, int len) throws IOException {
            int count = 0;
            while (!isEOF() && len > 0) {
                int rc = current.read(b, off, len);
                if (rc <= 0) {
                    doNext();
                    continue;
                }
                off += rc;
                len -= rc;
                count += rc;
            }
            return count == 0 ? (isEOF() ? -1 : 0) : count;
        }

        @Override
		public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
		public long skip(long n) throws IOException {
            while (!isEOF() && n > 0) {
                n -= current.skip(n);
            }
            return n;
        }

    }

    static String checkDefaultPrefix(ExecType execType, String fileSpec) {
        if (fileSpec.startsWith(LOCAL_PREFIX))
            return fileSpec;
        return (execType == ExecType.LOCAL ? LOCAL_PREFIX : "") + fileSpec;
    }

    /**
     * This function is meant to be used if the mappers/reducers want to access any HDFS file
     * @param fileName
     * @return
     * @throws IOException
     */
    
    public static InputStream openDFSFile(String fileName) throws IOException{
    	PigRecordReader prr = PigInputFormat.PigRecordReader.getPigRecordReader();
		
		if (prr == null)
			throw new RuntimeException("can't open DFS file while executing locally");
	
		return openDFSFile(fileName, prr.getJobConf());
		
    }

    public static InputStream openDFSFile(String fileName, JobConf conf) throws IOException{
    	Path path = new Path(fileName);
		FileSystem fs = path.getFileSystem(conf);
		return  openDFSFile(fs, path);
    }
    
    private static InputStream openDFSFile(FileSystem fs, Path path) throws IOException{
       Path paths[] = null;
    	if (fs.exists(path)) {
    		if (fs.isFile(path)) return fs.open(path);
			FileStatus fileStat[] = fs.listStatus(path);
			paths = new Path[fileStat.length];
			for (int i = 0; i < fileStat.length; i++)
        		paths[i] = fileStat[i].getPath();
		} else {
			// It might be a glob
			if (!globMatchesFiles(path, paths, fs)) throw new IOException(path + " does not exist");
		}
        return new DFSInputStreamIterator(paths, fs);
    }
    
    static public InputStream open(String fileSpec, PigContext pigContext) throws IOException {
        fileSpec = checkDefaultPrefix(pigContext.getExecType(), fileSpec);
        if (!fileSpec.startsWith(LOCAL_PREFIX)) {
            init(pigContext);
            Path path = new Path(fullPath(fileSpec,pigContext));
            return openDFSFile(pigContext.getDfs(), path);
        } else {
            fileSpec = fileSpec.substring(LOCAL_PREFIX.length());
            //buffering because we only want buffered streams to be passed to load functions.
            return new BufferedInputStream(new FileInputStream(fileSpec));
        }
    }
    
    static public OutputStream create(String fileSpec, PigContext pigContext) throws IOException{
    	return create(fileSpec,false,pigContext);
    }

    static public OutputStream create(String fileSpec, boolean append, PigContext pigContext) throws IOException {
        fileSpec = checkDefaultPrefix(pigContext.getExecType(), fileSpec);
        if (!fileSpec.startsWith(LOCAL_PREFIX)) {
            init(pigContext);
            return pigContext.getDfs().create(new Path(fileSpec));
        } else {
        	fileSpec = fileSpec.substring(LOCAL_PREFIX.length());
        	
        	File f = (new File(fileSpec)).getParentFile();
        	if (f!=null){
        		f.mkdirs();
        	}
        	
            return new FileOutputStream(fileSpec,append);
        }

    }

    static Stack<Path> toDelete    = new Stack<Path>();
    static Random      r           = new Random();
    static Path        relativeRoot;
    static boolean     initialized = false;
    static private void init(final PigContext pigContext) {
        if (!initialized) {
            initialized = true;
            relativeRoot = new Path("/tmp/temp" + r.nextInt());
            toDelete.push(relativeRoot);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
				public void run() {
                    while (!toDelete.empty()) {
                        try {
                            Path path = toDelete.pop();
                            pigContext.getDfs().delete(path);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }

    public static synchronized Path getTemporaryPath(Path relative, PigContext pigContext) throws IOException {
        init(pigContext);
        if (relative == null) {
            relative = relativeRoot;
        }
        if (!pigContext.getDfs().exists(relativeRoot))
            pigContext.getDfs().mkdirs(relativeRoot);
        Path path = new Path(relative, "tmp" + r.nextInt());
        toDelete.push(path);
        return path;
    }

    public static String hadoopify(String filename, PigContext pigContext) throws IOException {
        if (filename.startsWith(LOCAL_PREFIX)) {
            filename = filename.substring(LOCAL_PREFIX.length());
        }
        Path localPath = new Path(filename);
        if (!pigContext.getLfs().exists(localPath)) {
            throw new FileNotFoundException(filename);
        }
        Path newPath = getTemporaryPath(null, pigContext);
        int suffixStart = filename.lastIndexOf('.');
        if (suffixStart != -1) {
            newPath = newPath.suffix(filename.substring(suffixStart));
        }
        boolean success = FileUtil.copy(pigContext.getLfs(), new Path(filename), pigContext.getDfs(), newPath, false, pigContext.getConf());
        if (!success) {
            throw new IOException("Couldn't copy " + filename);
        }
        return newPath.toString();
    }

    public static String fullPath(String filename, PigContext pigContext) throws IOException {
	try
	{
        	if (filename.charAt(0) != '/') {
            	return new Path(pigContext.getDfs().getWorkingDirectory(), filename).toString();
        	}
        	return filename;
	}
	catch (IllegalArgumentException e)
	{
		return filename;
	}
    }

    public static boolean fileExists(String filename, PigContext pigContext) throws IOException {
	try
	{
		Path path = new Path(filename);
		FileSystem fs = pigContext.getDfs();
		if (fs.exists(path)) return true;
		else return globMatchesFiles(path, fs);
	}
	catch (IllegalArgumentException e)
	{
		return false;
	}
    }

	private static boolean globMatchesFiles(Path p, FileSystem fs)
		throws IOException
	{
		Path[] paths = null;
		return globMatchesFiles(p, paths, fs);
	}

	private static boolean globMatchesFiles(Path p, Path[] paths, FileSystem fs)
		throws IOException
	{
		// Currently, if you give a glob with non-special glob characters, hadoop
		// returns an array with your file name in it.  So check for that.
		paths = fs.globPaths(p);
		switch (paths.length) {
		case 0:
			return false;

		case 1:
			return !paths[0].equals(p);

		default:
			return true;
		}
	}

}
