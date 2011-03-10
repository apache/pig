/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.piggybank.storage.allloader;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.pig.FuncSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * 
 * Contains the logic for finding a LoadFunc based on the definition of:
 * <ul>
 * <li>file.extension.loaders</li>
 * <li>file.format.loaders</li>
 * </ul>
 * 
 */
public class LoadFuncHelper {

    public static final String FILE_EXTENSION_LOADERS = "file.extension.loaders";

    /**
     * The most common type of file formats are supported i.e. SEQ, GZ, BZ2, LZO
     * This is used when a file does not have an extension. If this is true the
     * first 3 bytes can be read from a file to determine its content type. The
     * 3 bytes are then mapped to an extension for which an entry must exist in
     * file.extension.loaders. If the content does not match the any entry in
     * magicNumberExtensionMap plain text is assumed.
     */
    private static Map<MagicNumber, String> magicNumberExtensionMap = buildMagicNumberExtensionMap();

    Configuration conf;

    FileSystem fileSystem;

    /**
     * Stores the extension:tag = load function pairs
     */
    Map<String, String> loadFunctionExtensionTagMap;

    /**
     * Stores the extension = tags pairs
     */
    Map<String, Set<String>> extensionTagsMap;

    public LoadFuncHelper(Configuration conf) throws IOException {
        this.conf = conf;
        fileSystem = FileSystem.get(conf);

        loadFunctionExtensionTagMap = new HashMap<String, String>();
        extensionTagsMap = new HashMap<String, Set<String>>();

        String fileExtensionLoaders = conf.get(FILE_EXTENSION_LOADERS);

        if (fileExtensionLoaders != null) {
            String[] loaderExtensionPairs = fileExtensionLoaders.split("\\),");
            for (String loaderExtensionPairStr : loaderExtensionPairs) {

                String[] loaderExtensionPair = loaderExtensionPairStr
                        .split(":");
                if (loaderExtensionPair.length == 2) {
                    // we have extension:loader assign EMPTY TAG
                    loadFunctionExtensionTagMap.put(
                            loaderExtensionPair[0].trim() + ":",
                            loaderExtensionPair[1].trim());
                } else if (loaderExtensionPair.length == 3
                        || loaderExtensionPair.length == 4) {
                    // we have extension:pathTag:loader assign TAG
                    String ext = loaderExtensionPair[0].trim();
                    String tag = loaderExtensionPair[1].trim();

                    String key = ext + ":" + tag;

                    String loadFunc = loaderExtensionPair[2].trim();
                    // support key class names for sequence files
                    if (loaderExtensionPair.length == 4) {
                        // loadFunc here is not loadFunc but the sequence file
                        // key class
                        key += ":" + loadFunc;
                        loadFunc = loaderExtensionPair[3].trim();
                    }

                    loadFunctionExtensionTagMap.put(key, loadFunc);

                    Set<String> tags = extensionTagsMap.get(ext);
                    if (tags == null) {
                        tags = new TreeSet<String>();
                        extensionTagsMap.put(ext, tags);
                    }

                    tags.add(tag);

                } else {
                    throw new FrontendException(
                            "Bad formatted file.extension.loaders string, format is <extension>:<loader>,<extenion><loader>");
                }
            }
        }

    }

    /**
     * 
     * @return
     */
    private static Map<MagicNumber, String> buildMagicNumberExtensionMap() {
        Map<MagicNumber, String> magicNumberExtensionMap = new HashMap<MagicNumber, String>();
        magicNumberExtensionMap.put(new MagicNumber(new byte[] { 83, 69, 81 }),
                "seq");
        magicNumberExtensionMap.put(
                new MagicNumber(new byte[] { -119, 76, 90 }), "lzo");
        magicNumberExtensionMap.put(
                new MagicNumber(new byte[] { 31, -117, 8 }), "gz");
        magicNumberExtensionMap.put(
                new MagicNumber(new byte[] { 66, 90, 104 }), "bz2");

        return magicNumberExtensionMap;
    }

    /**
     * If location is a directory the first file found is returned
     * 
     * @param location
     * @return
     * @throws IOException
     *             if no file is found a FrontendException is thrown
     */
    public Path determineFirstFile(String location) throws IOException {
        Path path = new Path(location);
        FileStatus status = fileSystem.getFileStatus(path);

        if (status.isDir()) {
            // get the first file.
            path = getFirstFile(fileSystem, path);

            if (path == null) {
                throw new FrontendException(path + " has no files");
            }
        }

        return path;
    }

    /**
     * 
     * If location is a directory the first file found in the directory is used.<br/>
     * The file extension of the file will be searched against the
     * file.extension.loaders mappings. If none found null is returned.
     * 
     * @param location
     * @return
     * @throws IOException
     */
    public FuncSpec determineFunction(String location) throws IOException {
        return determineFunction(location, determineFirstFile(location));
    }

    /**
     * 
     * The file extension of the file will be searched against the
     * file.extension.loaders mappings. If none found null is returned.
     * 
     * @param path
     * @param location
     * @return
     * @throws IOException
     */
    public FuncSpec determineFunction(String location, Path path)
            throws IOException {

        String fileName = path.getName();

        FuncSpec funcSpec = getLoadPerExtension(fileName, path);

        if (funcSpec == null) {
            // look for loaders by the content definition

            funcSpec = getFuncSpecFromContent(path);

        }

        return funcSpec;
    }

    /**
     * Tries to identify the extension and there by the loader from the content
     * type.
     * 
     * @param path
     * @return
     * @throws IOException
     */
    private FuncSpec getFuncSpecFromContent(Path path) throws IOException {
        // get the first three bytes from the file.
        FSDataInputStream dataIn = null;
        byte[] magic = new byte[3];
        int read = -1;

        try {
            dataIn = fileSystem.open(path, 3);
            read = dataIn.read(magic);
        } finally {
            dataIn.close();
        }

        FuncSpec funcSpec = null;
        String extensionMapping = magicNumberExtensionMap.get(new MagicNumber(
                magic));

        if (read < magic.length || extensionMapping == null) {
            // assume plain text
            funcSpec = new FuncSpec("PigStorage()");
        } else {
            // an extension mapping was found. i.e. this is a GZ, BZ2, LZO or
            // SEQ file

            String applicableTag = getApplicableTag(extensionMapping, path);
            String loadFuncDefinition = null;

            if (extensionMapping.equals("seq")) {
                // if this is a sequence file we load the key class also
                loadFuncDefinition = loadFunctionExtensionTagMap
                        .get(extensionMapping + ":" + applicableTag + ":"
                                + getSequenceFileKeyClass(path));

            }

            // we do this also for sequence file because a sequence file might
            // have a sequeyceFileKey associated or not in the extension mapping
            // given both cases if the key class is not found above in the
            // mapping, the default sequence file loader needs to be used as per
            // the extension mapping.
            if (loadFuncDefinition == null) {
                // use only extension and tag filtering
                loadFuncDefinition = loadFunctionExtensionTagMap
                        .get(extensionMapping + ":" + applicableTag);

            }

            if (loadFuncDefinition == null) {
                // if still null thrown an error
                throw new RuntimeException("Cannot find loader for " + path
                        + " extension mapping " + extensionMapping);
            }

            funcSpec = new FuncSpec(loadFuncDefinition);
        }

        return funcSpec;
    }

    /**
     * Open a SequenceFile.Reader instance and return the keyClassName
     * 
     * @param path
     * @return
     * @throws IOException
     */
    private String getSequenceFileKeyClass(Path path) throws IOException {

        String keyClassName = null;
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path,
                conf);

        try {

            keyClassName = reader.getKeyClassName();
            int index = keyClassName.indexOf("$");
            if (index > 0) {
                keyClassName = keyClassName.substring(0, index);
            }

        } finally {
            reader.close();
        }

        return keyClassName;
    }

    /**
     * Search for the correct loader based on the extension and tags mappings.
     * 
     * @param fileName
     * @param path
     * @return
     */
    private FuncSpec getLoadPerExtension(String fileName, Path path) {

        String extension = null;
        String applicableTag = null;
        String loadFuncDefinition = null;
        FuncSpec funcSpec = null;

        // NOTE: the inverse logic !( a == null && b == null) is not used
        // because we want all statements to be cheked as long as they are not
        // null.
        while (fileName != null && (extension = getExtension(fileName)) != null
                && (applicableTag = getApplicableTag(extension, path)) != null) {

            if ((loadFuncDefinition = loadFunctionExtensionTagMap.get(extension
                    + ":" + applicableTag)) != null) {
                // create the LoadFunc
                funcSpec = new FuncSpec(loadFuncDefinition);
                break;
            }

            fileName = cutExtension(fileName);
        }

        return funcSpec;
    }

    /**
     * Searches in the path for the first occurrence of the tags associated with
     * the extension.<br/>
     * If this extension has no tags an empty string is returned.<br/>
     * If it has tags and no tag is found in the path null is returned.<br/>
     * 
     * @param extension
     * @param path
     * @return
     */
    private String getApplicableTag(String extension, Path path) {

        Set<String> tags = extensionTagsMap.get(extension);
        String applicableTag = null;

        if (tags != null) {

            String fullPathName = path.toUri().toString();

            for (String tag : tags) {
                if (fullPathName.contains(tag)) {
                    applicableTag = tag;
                    break;
                }
            }

        } else {
            applicableTag = "";
        }

        return applicableTag;
    }

    /**
     * @param fileName
     * @return String return the file name without the last extension e.g.
     *         file.rc.gz will return file.rc
     */
    private static String cutExtension(String fileName) {
        String name = null;

        int index = fileName.lastIndexOf('.');
        if (index > 0 && index < fileName.length()) {
            name = fileName.substring(0, index);
        }

        return name;
    }

    /**
     * 
     * @param fileName
     * @return String return the last file name extension e.g. file.rc.gz will
     *         return gz
     */
    private static String getExtension(String fileName) {

        String extension = null;

        int index = fileName.lastIndexOf('.');
        int pos = index + 1;
        if (index > 0 && pos < fileName.length()) {
            extension = fileName.substring(pos, fileName.length());
        }

        return extension;
    }

    /**
     * Looks for and returns the first file it can find.
     * 
     * @return Path null is no file was found
     * @throws IOException
     */
    private static Path getFirstFile(FileSystem fileSystem, Path path)
            throws IOException {
        Path currentPath = path;
        Path file = null;

        FileStatus[] paths = fileSystem.listStatus(currentPath);
        Arrays.sort(paths);

        for (FileStatus subPathStatus : paths) {

            currentPath = subPathStatus.getPath();

            // if hidden file skip.
            if (currentPath.getName().startsWith(".")
                    || currentPath.getName().startsWith("_")) {
                continue;
            }

            if (subPathStatus.isDir()) {
                file = getFirstFile(fileSystem, currentPath);
            } else {
                // first file found return.
                file = currentPath;
                break;
            }
        }

        return file;
    }

    static class MagicNumber {

        byte[] magic;

        public MagicNumber(byte[] magic) {
            super();
            this.magic = magic;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(magic);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            MagicNumber other = (MagicNumber) obj;
            if (!Arrays.equals(magic, other.magic))
                return false;
            return true;
        }

    }
}
