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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.Properties ;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.ExecType;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.SliceWrapper;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;

public class FileLocalizer {
    private static final Log log = LogFactory.getLog(FileLocalizer.class);
    
    static public final String LOCAL_PREFIX  = "file:";
    static public final int STYLE_UNIX = 0;
    static public final int STYLE_WINDOWS = 1;

    public static class DataStorageInputStreamIterator extends InputStream {
        InputStream current;
        ElementDescriptor[] elements;
        int currentElement;
        
        public DataStorageInputStreamIterator(ElementDescriptor[] elements) {
            this.elements = elements;
        }

        private boolean isEOF() throws IOException {
            if (current == null) {
                if (currentElement == elements.length) {
                    return true;
                }
                current = elements[ currentElement++ ].open();
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
            currentElement = elements.length;
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
     * @return InputStream of the open file.
     * @throws IOException
     */
    public static InputStream openDFSFile(String fileName) throws IOException {
        SliceWrapper wrapper = PigInputFormat.getActiveSplit();

        if (wrapper == null)
            throw new RuntimeException(
                    "can't open DFS file while executing locally");
        
        return openDFSFile(fileName, ConfigurationUtil.toProperties(wrapper.getJobConf()));

    }

    public static InputStream openDFSFile(String fileName, Properties properties) throws IOException{
        DataStorage dds = new HDataStorage(properties);
        ElementDescriptor elem = dds.asElement(fileName);
        return openDFSFile(elem);
    }
    
    private static InputStream openDFSFile(ElementDescriptor elem) throws IOException{
        ElementDescriptor[] elements = null;
        
        if (elem.exists()) {
            try {
                if(! elem.getDataStorage().isContainer(elem.toString())) {
                    if (elem.systemElement())
                        throw new IOException ("Attempt is made to open system file " + elem.toString());
                    return elem.open();
                }
            }
            catch (DataStorageException e) {
                throw WrappedIOException.wrap("Failed to determine if elem=" + elem + " is container", e);
            }
            
            // elem is a directory - recursively get all files in it
            elements = getFileElementDescriptors(elem);
        } else {
            // It might be a glob
            if (!globMatchesFiles(elem, elem.getDataStorage())) {
                throw new IOException(elem.toString() + " does not exist");
            } else {
                elements = getFileElementDescriptors(elem); 
                return new DataStorageInputStreamIterator(elements);
                
            }
        }
        
        return new DataStorageInputStreamIterator(elements);
    }
    
    /**
     * recursively get all "File" element descriptors present in the input element descriptor
     * @param elem input element descriptor
     * @return an array of Element descriptors for files present (found by traversing all levels of dirs)
     *  in the input element descriptor
     * @throws DataStorageException
     */
    private static ElementDescriptor[] getFileElementDescriptors(ElementDescriptor elem) throws DataStorageException {
        DataStorage store = elem.getDataStorage();
        ElementDescriptor[] elems = store.asCollection(elem.toString());
        // elems could have directories in it, if so
        // get the files out so that it contains only files
        List<ElementDescriptor> paths = new ArrayList<ElementDescriptor>();
        List<ElementDescriptor> filePaths = new ArrayList<ElementDescriptor>();
        for (int m = 0; m < elems.length; m++) {
            paths.add(elems[m]);
        }
        for (int j = 0; j < paths.size(); j++) {
            ElementDescriptor fullPath = store.asElement(store
                    .getActiveContainer(), paths.get(j));
            // Skip hadoop's private/meta files ...
            if (fullPath.systemElement()) {
                continue;
            }
            
            if (fullPath instanceof ContainerDescriptor) {
                for (ElementDescriptor child : ((ContainerDescriptor) fullPath)) {
                    paths.add(child);
                }
                continue;
            } else {
                // this is a file, add it to filePaths
                filePaths.add(fullPath);
            }
        }
        elems = new ElementDescriptor[filePaths.size()];
        filePaths.toArray(elems);
        return elems;
    }
    
    private static InputStream openLFSFile(ElementDescriptor elem) throws IOException{
        // IMPORTANT NOTE: Currently we use HXXX classes to represent
        // files and dirs in local mode - so we can just delegate this
        // call to openDFSFile(elem). When we have true local mode files
        // and dirs THIS WILL NEED TO CHANGE
        return openDFSFile(elem);
    }
    
    /**
     * This function returns an input stream to a local file system file or
     * a file residing on Hadoop's DFS
     * @param fileName The filename to open
     * @param execType execType indicating whether executing in local mode or MapReduce mode (Hadoop)
     * @param storage The DataStorage object used to open the fileSpec
     * @return InputStream to the fileSpec
     * @throws IOException
     */
    static public InputStream open(String fileName, ExecType execType, DataStorage storage) throws IOException {
        fileName = checkDefaultPrefix(execType, fileName);
        if (!fileName.startsWith(LOCAL_PREFIX)) {
            ElementDescriptor elem = storage.asElement(fullPath(fileName, storage));
            return openDFSFile(elem);
        }
        else {
            fileName = fileName.substring(LOCAL_PREFIX.length());
            ElementDescriptor elem = storage.asElement(fullPath(fileName, storage));
            return openLFSFile(elem);
        }
    }
    
    private static String fullPath(String fileName, DataStorage storage) {
        String fullPath;
        try {
            if (fileName.charAt(0) != '/') {
                ElementDescriptor currentDir = storage.getActiveContainer();
                ElementDescriptor elem = storage.asElement(currentDir.toString(), fileName);
                
                fullPath = elem.toString();
            } else {
                fullPath = fileName;
            }
        } catch (DataStorageException e) {
            fullPath = fileName;
        }
        return fullPath;
    }
    
    static public InputStream open(String fileSpec, PigContext pigContext) throws IOException {
        fileSpec = checkDefaultPrefix(pigContext.getExecType(), fileSpec);
        if (!fileSpec.startsWith(LOCAL_PREFIX)) {
            init(pigContext);
            ElementDescriptor elem = pigContext.getDfs().asElement(fullPath(fileSpec, pigContext));
            return openDFSFile(elem);
        }
        else {
            fileSpec = fileSpec.substring(LOCAL_PREFIX.length());
            //buffering because we only want buffered streams to be passed to load functions.
            /*return new BufferedInputStream(new FileInputStream(fileSpec));*/
            init(pigContext);
            ElementDescriptor elem = pigContext.getLfs().asElement(fullPath(fileSpec, pigContext));
            return openLFSFile(elem);
        }
    }
    
    static public OutputStream create(String fileSpec, PigContext pigContext) throws IOException{
        return create(fileSpec,false,pigContext);
    }

    static public OutputStream create(String fileSpec, boolean append, PigContext pigContext) throws IOException {
        fileSpec = checkDefaultPrefix(pigContext.getExecType(), fileSpec);
        if (!fileSpec.startsWith(LOCAL_PREFIX)) {
            init(pigContext);
            ElementDescriptor elem = pigContext.getDfs().asElement(fileSpec);
            return elem.create();
        }
        else {
            fileSpec = fileSpec.substring(LOCAL_PREFIX.length());

            // TODO probably this should be replaced with the local file system
            File f = (new File(fileSpec)).getParentFile();
            if (f!=null){
                f.mkdirs();
            }
            
            return new FileOutputStream(fileSpec,append);
        }
    }
    
    static public boolean delete(String fileSpec, PigContext pigContext) throws IOException{
        fileSpec = checkDefaultPrefix(pigContext.getExecType(), fileSpec);
        if (!fileSpec.startsWith(LOCAL_PREFIX)) {
            init(pigContext);
            ElementDescriptor elem = pigContext.getDfs().asElement(fileSpec);
            elem.delete();
            return true;
        }
        else {
            fileSpec = fileSpec.substring(LOCAL_PREFIX.length());
            boolean ret = true;
            // TODO probably this should be replaced with the local file system
            File f = (new File(fileSpec));
            // TODO this only deletes the file. Any dirs createdas a part of this
            // are not removed.
            if (f!=null){
                ret = f.delete();
            }
            
            return ret;
        }
    }

    static Stack<ElementDescriptor> toDelete    = 
        new Stack<ElementDescriptor>();
    static Stack<ElementDescriptor> deleteOnFail    = 
        new Stack<ElementDescriptor>();
    static Random      r           = new Random();
    static ContainerDescriptor relativeRoot;
    static boolean     initialized = false;
    static private void init(final PigContext pigContext) throws DataStorageException {
        if (!initialized) {
            initialized = true;
            relativeRoot = pigContext.getDfs().asContainer("/tmp/temp" + r.nextInt());
            toDelete.push(relativeRoot);
            // Runtime.getRuntime().addShutdownHook(new Thread() {
              //   @Override
            //     public void run() {
            //          deleteTempFiles();
            //     }
            //});
        }
    }

    public static void deleteTempFiles() {
        while (!toDelete.empty()) {
            try {
                ElementDescriptor elem = toDelete.pop();
                elem.delete();
            } 
            catch (IOException e) {
                log.error(e);
            }
        }
        initialized = false;
    }

    public static synchronized ElementDescriptor 
        getTemporaryPath(ElementDescriptor relative, 
                         PigContext pigContext) throws IOException {
        init(pigContext);
        if (relative == null) {
            relative = relativeRoot;
        }
        if (!relativeRoot.exists()) {
            relativeRoot.create();
        }
        ElementDescriptor elem= 
            pigContext.getDfs().asElement(relative.toString(), "tmp" + r.nextInt());
        toDelete.push(elem);
        return elem;
    }

    public static String hadoopify(String filename, PigContext pigContext) throws IOException {
        if (filename.startsWith(LOCAL_PREFIX)) {
            filename = filename.substring(LOCAL_PREFIX.length());
        }
        
        ElementDescriptor localElem =
            pigContext.getLfs().asElement(filename);
            
        if (!localElem.exists()) {
            throw new FileNotFoundException(filename);
        }
            
        ElementDescriptor distribElem = 
            getTemporaryPath(null, pigContext);
    
        int suffixStart = filename.lastIndexOf('.');
        if (suffixStart != -1) {
            distribElem = pigContext.getDfs().asElement(distribElem.toString() +
                    filename.substring(suffixStart));
        }
            
        // TODO: currently the copy method in Data Storage does not allow to specify overwrite
        //       so the work around is to delete the dst file first, if it exists
        if (distribElem.exists()) {
            distribElem.delete();
        }
        localElem.copy(distribElem, null, false);
            
        return distribElem.toString();
    }

    public static String fullPath(String filename, PigContext pigContext) throws IOException {
        try {
            if (filename.charAt(0) != '/') {
                ElementDescriptor currentDir = pigContext.getDfs().getActiveContainer();
                ElementDescriptor elem = pigContext.getDfs().asElement(currentDir.toString(),
                                                                                  filename);
                
                return elem.toString();
            }
            return filename;
        }
        catch (DataStorageException e) {
            return filename;
        }
    }

    public static boolean fileExists(String filename, PigContext context)
            throws IOException {
        return fileExists(filename, context.getFs());
    }

    public static boolean fileExists(String filename, DataStorage store)
            throws IOException {
        ElementDescriptor elem = store.asElement(filename);
        return elem.exists() || globMatchesFiles(elem, store);
    }

    public static boolean isFile(String filename, PigContext context)
    throws IOException {
        return !isDirectory(filename, context.getDfs());
    }

    public static boolean isFile(String filename, DataStorage store)
    throws IOException {
        return !isDirectory(filename, store);
    }

    public static boolean isDirectory(String filename, PigContext context)
    throws IOException {
        return isDirectory(filename, context.getDfs());
    }

    public static boolean isDirectory(String filename, DataStorage store)
    throws IOException {
        ElementDescriptor elem = store.asElement(filename);
        return (elem instanceof ContainerDescriptor);
    }

    private static boolean globMatchesFiles(ElementDescriptor elem,
                                            DataStorage fs)
            throws IOException
    {
        try {
            // Currently, if you give a glob with non-special glob characters, hadoop
            // returns an array with your file name in it.  So check for that.
            ElementDescriptor[] elems = fs.asCollection(elem.toString());
            switch (elems.length) {
            case 0:
                return false;
    
            case 1:
                return !elems[0].equals(elem);
    
            default:
                return true;
            }
        }
        catch (DataStorageException e) {
            //throw WrappedIOException.wrap("Unable to get collect for pattern " + elem.toString(), e);
            throw e;
        }
    }

    public static Random getR() {
        return r;
    }

    public static void setR(Random r) {
        FileLocalizer.r = r;
    }
    
    public static void clearDeleteOnFail()
    {
    	deleteOnFail.clear();
    }
    public static void registerDeleteOnFail(String filename, PigContext pigContext) throws IOException
    {
    	try {
    		ElementDescriptor elem = pigContext.getDfs().asElement(filename);
    		if (!toDelete.contains(elem))
    		    deleteOnFail.push(elem);
    	}
        catch (DataStorageException e) {
            log.warn("Unable to register output file to delete on failure: " + filename);
        }
    }
    public static void triggerDeleteOnFail()
    {
    	ElementDescriptor elem = null;
    	while (!deleteOnFail.empty()) {
            try {
                elem = deleteOnFail.pop();
                if (elem.exists())
                	elem.delete();
            } 
            catch (IOException e) {
                log.warn("Unable to delete output file on failure: " + elem.toString());
            }
    	}
    }
    /**
     * Convert path from Windows convention to Unix convention. Invoked under
     * cygwin.
     * 
     * @param path
     *            path in Windows convention
     * @return path in Unix convention, null if fail
     */
    static public String parseCygPath(String path, int style) {
        String[] command; 
        if (style==STYLE_WINDOWS)
            command = new String[] { "cygpath", "-w", path };
        else
            command = new String[] { "cygpath", "-u", path };
        Process p = null;
        try {
            p = Runtime.getRuntime().exec(command);
        } catch (IOException e) {
            return null;
        }
        int exitVal = 0;
        try {
            exitVal = p.waitFor();
        } catch (InterruptedException e) {
            return null;
        }
        if (exitVal != 0)
            return null;
        String line = null;
        try {
            InputStreamReader isr = new InputStreamReader(p.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            line = br.readLine();
        } catch (IOException e) {
            return null;
        }
        return line;
    }
}
