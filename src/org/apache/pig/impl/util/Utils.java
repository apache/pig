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
package org.apache.pig.impl.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.InterStorage;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.io.TFileStorage;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.parser.ParserException;
import org.apache.pig.parser.QueryParserDriver;

import com.google.common.collect.Lists;

/**
 * Class with utility static methods
 */
public class Utils {
	private static final Log log = LogFactory.getLog(Utils.class);
    /**
     * This method is a helper for classes to implement {@link java.lang.Object#equals(java.lang.Object)}
     * checks if two objects are equals - two levels of checks are
     * made - first if both are null or not null. If either is null,
     * check is made whether both are null.
     * If both are non null, equality also is checked if so indicated
     * @param obj1 first object to be compared
     * @param obj2 second object to be compared
     * @param checkEquality flag to indicate whether object equality should
     * be checked if obj1 and obj2 are non-null
     * @return true if the two objects are equal
     * false otherwise
     */
    public static boolean checkNullEquals(Object obj1, Object obj2, boolean checkEquality) {
        if(obj1 == null || obj2 == null) {
            return obj1 == obj2;
        }
        if(checkEquality) {
            if(!obj1.equals(obj2)) {
                return false;
            }
        }
        return true;
    }


    /**
     * This method is a helper for classes to implement {@link java.lang.Object#equals(java.lang.Object)}
     * The method checks whether the two arguments are both null or both not null and 
     * whether they are of the same class
     * @param obj1 first object to compare
     * @param obj2 second object to compare
     * @return true if both objects are null or both are not null
     * and if both are of the same class if not null
     * false otherwise
     */
    public static boolean checkNullAndClass(Object obj1, Object obj2) {
        if(checkNullEquals(obj1, obj2, false)) {
            if(obj1 != null) {
                return obj1.getClass() == obj2.getClass();
            } else {
                return true; // both obj1 and obj2 should be null
            }
        } else {
            return false;
        }
    }

    /**
     * A helper function for retrieving the script schema set by the LOLoad
     * function.
     * 
     * @param loadFuncSignature
     * @param conf
     * @return Schema
     * @throws IOException
     */
    public static Schema getScriptSchema(
            String loadFuncSignature,
            Configuration conf) throws IOException {
        Schema scriptSchema = null;
        String scriptField = conf.get(getScriptSchemaKey(loadFuncSignature));

        if (scriptField != null) {
            scriptSchema = (Schema) ObjectSerializer.deserialize(scriptField);
        }

        return scriptSchema;
    }

    public static String getScriptSchemaKey(String loadFuncSignature) {
      return loadFuncSignature + ".scriptSchema";
    }

    public static ResourceSchema getSchema(LoadFunc wrappedLoadFunc, String location, boolean checkExistence, Job job)
    throws IOException {
        Configuration conf = job.getConfiguration();
        if (checkExistence) {
            Path path = new Path(location);
            if (!FileSystem.get(conf).exists(path)) {
                // At compile time in batch mode, the file may not exist
                // (such as intermediate file). Just return null - the
                // same way as we would if we did not get a valid record
                return null;
            }
        }
        ReadToEndLoader loader = new ReadToEndLoader(wrappedLoadFunc, conf, location, 0);
        // get the first record from the input file
        // and figure out the schema from the data in
        // the first record
        Tuple t = loader.getNext();
        if (t == null) {
            // we couldn't get a valid record from the input
            return null;
        }
        int numFields = t.size();
        Schema s = new Schema();
        for (int i = 0; i < numFields; i++) {
            try {
                s.add(DataType.determineFieldSchema(t.get(i)));
            }
            catch (Exception e) {
                int errCode = 2104;
                String msg = "Error while determining schema of SequenceFileStorage data.";
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        }
        return new ResourceSchema(s);
    }

    /**
     * @param schemaString
     * @return Schema instance
     * @throws ParserException
     */
    public static Schema getSchemaFromString(String schemaString) throws ParserException {
        LogicalSchema schema = parseSchema(schemaString);
        Schema result = org.apache.pig.newplan.logical.Util.translateSchema(schema);
        Schema.setSchemaDefaultType(result, DataType.BYTEARRAY);
        return result;
    }

	public static LogicalSchema parseSchema(String schemaString) throws ParserException {
		QueryParserDriver queryParser = new QueryParserDriver( new PigContext(), 
        		"util", new HashMap<String, String>() ) ;
        LogicalSchema schema = queryParser.parseSchema(schemaString);
		return schema;
	}
	
    /**
     * This method adds FieldSchema of 'input source tag/path' as the first
     * field. This will be called only when PigStorage is invoked with
     * '-tagsource' option and the schema file is present to be loaded.
     * 
     * @param schema
     * @return ResourceSchema
     */
    public static ResourceSchema getSchemaWithInputSourceTag(ResourceSchema schema) {
        ResourceFieldSchema[] fieldSchemas = schema.getFields();
        ResourceFieldSchema sourceTagSchema = new ResourceFieldSchema(new FieldSchema("INPUT_FILE_NAME", DataType.CHARARRAY));
        ResourceFieldSchema[] fieldSchemasWithSourceTag = new ResourceFieldSchema[fieldSchemas.length + 1];
        fieldSchemasWithSourceTag[0] = sourceTagSchema;
        for(int j = 0; j < fieldSchemas.length; j++) {
            fieldSchemasWithSourceTag[j + 1] = fieldSchemas[j];
        }
        return schema.setFields(fieldSchemasWithSourceTag);
    }

    public static String getTmpFileCompressorName(PigContext pigContext) {
        if (pigContext == null)
            return InterStorage.class.getName();
        boolean tmpFileCompression = pigContext.getProperties().getProperty("pig.tmpfilecompression", "false").equals("true");
        String codec = pigContext.getProperties().getProperty("pig.tmpfilecompression.codec", "");
        if (tmpFileCompression) {
            if (codec.equals("lzo"))
                pigContext.getProperties().setProperty("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
            return TFileStorage.class.getName();
        } else
            return InterStorage.class.getName();
    }

    public static FileInputLoadFunc getTmpFileStorageObject(Configuration conf) throws IOException {
        boolean tmpFileCompression = conf.getBoolean("pig.tmpfilecompression", false);
        return tmpFileCompression ? new TFileStorage() : new InterStorage();
    }

    public static boolean tmpFileCompression(PigContext pigContext) {
        if (pigContext == null)
            return false;
        return pigContext.getProperties().getProperty("pig.tmpfilecompression", "false").equals("true");
    }

    public static String tmpFileCompressionCodec(PigContext pigContext) throws IOException {
        if (pigContext == null)
            return "";
        String codec = pigContext.getProperties().getProperty("pig.tmpfilecompression.codec", "");
        if (codec.equals("gz") || codec.equals("lzo"))
            return codec;
        else
            throw new IOException("Invalid temporary file compression codec ["+codec+"]. Expected compression codecs are gz and lzo");
    }

    public static String getStringFromArray(String[] arr) {
        StringBuilder str = new StringBuilder();
        for(String s: arr) {
            str.append(s);
            str.append(" ");
        }
        return str.toString();
    }

    public static FuncSpec buildSimpleFuncSpec(String className, byte...types) {
        List<Schema.FieldSchema> fieldSchemas = Lists.newArrayListWithExpectedSize(types.length);
        for (byte type : types) {
            fieldSchemas.add(new Schema.FieldSchema(null, type));
        }
        return new FuncSpec(className, new Schema(fieldSchemas));
    }

    /**
     * Replace sequences of two slashes ("\\") with one slash ("\")
     * (not escaping a slash in grunt is disallowed, but a double slash doesn't get converted
     * into a regular slash, so we have to do it instead)
     * @param str
     * @return the resulting string
     */
    public static String slashisize(String str) {
        return str.replace("\\\\", "\\");
    }
    
    @SuppressWarnings("unchecked")
    public static <O> Collection<O> mergeCollection(Collection<O> a, Collection<O> b) {
        if (a==null && b==null)
            return null;
        Collection<O> result = null;
        try {
            if (a!=null)
                result = a.getClass().newInstance();
            else
                result = b.getClass().newInstance();
        } catch (Exception e) {
            // Shall not happen
        }
        if (a==null) {
            result.addAll(b);
        }
        else if (b==null) {
            result.addAll(a);
        }
        else {
            result.addAll(a);
            for (O o : b) {
                if (!result.contains(o)) {
                    result.add(o);
                }
            }
        }
        
        return result;
    }
    
   public static InputStream getCompositeStream(InputStream in, Properties properties) {
	   //Load default ~/.pigbootup if not specified by user
    	final String bootupFile = properties.getProperty("pig.load.default.statements", System.getProperty("user.home") + "/.pigbootup");
    	try {
    	final InputStream inputSteam = new FileInputStream(new File(bootupFile));
    	return new SequenceInputStream(inputSteam, in);
    	} catch(FileNotFoundException fe) {
    		log.info("Default bootup file " +bootupFile+ " not found");
    		return in;
    	}
    }

    /**
     * Returns the total number of bytes for this file, or if a file all files in the directory.
     */
    public static long getPathLength(FileSystem fs, FileStatus status) throws IOException {
        if (!status.isDir()) {
            return status.getLen();
        } else {
            FileStatus[] children = fs.listStatus(status.getPath());
            long size = 0;
            for (FileStatus child : children) {
                size += getPathLength(fs, child);
            }
            return size;
        }
    }

}
