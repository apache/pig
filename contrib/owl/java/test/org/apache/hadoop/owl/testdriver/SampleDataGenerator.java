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

package org.apache.hadoop.owl.testdriver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import net.sf.json.JsonConfig;
import net.sf.json.util.EnumMorpher;
import net.sf.json.util.JSONUtils;
import net.sf.json.util.NewBeanInstanceStrategy;
import net.sf.json.util.PropertyFilter;

public class SampleDataGenerator {

    private static final int MAX_TUPLES_IN_BAG = 3;
    private static final int MAX_ENTRIES_IN_MAP = 3;

    private OwlSchema schema;
    private String keyPrefix;
    private int numPartFiles;
    private List<Tuple> tuples;
    private Path path;

    public SampleDataGenerator(OwlSchema schema, String keyPrefix,
            int numPartFiles, List<Tuple> tuples, Path path) {
        this.setSchema(schema);
        this.setKeyPrefix(keyPrefix);
        this.setNumPartFiles(numPartFiles);
        this.setTuples(tuples);
        this.setPath(path);
    }

    public SampleDataGenerator(OwlSchema schema, String keyPrefix,
            int numPartFiles, int numRows, Path path) throws ExecException, OwlException {
        this(schema,keyPrefix, numPartFiles, generateTuples(numRows,schema), path);
    }


    public static List<Tuple> generateTuples(int numTuples, OwlSchema schema) throws ExecException, OwlException {
        List<Tuple> tuples = new ArrayList<Tuple>();
        for (int i = 0; i < numTuples ; i++){
            tuples.add(generateTuple(schema));
        }
        return tuples;
    }

    public static Tuple generateTuple(OwlSchema schema) throws ExecException, OwlException{

        Tuple tp = TupleFactory.getInstance().newTuple(schema.getColumnCount());
        int columnNum = 0;
        for ( OwlColumnSchema columnSchema : schema.getColumnSchema()){
            ColumnType type = columnSchema.getType();
            if (type.equals(ColumnType.BOOL)){
                tp.set(columnNum, generateBool());
            } else if (type.equals(ColumnType.BYTES)) {
                tp.set(columnNum, generateBytes());
            } else if (type.equals(ColumnType.DOUBLE)) {
                tp.set(columnNum, generateDouble());
            } else if (type.equals(ColumnType.FLOAT)) {
                tp.set(columnNum, generateFloat());
            } else if (type.equals(ColumnType.INT)) {
                tp.set(columnNum, generateInt());
            } else if (type.equals(ColumnType.LONG)) {
                tp.set(columnNum, generateLong());
            } else if (type.equals(ColumnType.STRING)) {
                tp.set(columnNum, generateString());
            } else if (type.equals(ColumnType.COLLECTION)) {
                tp.set(columnNum, generateCollection(columnSchema.getSchema()));
            } else if (type.equals(ColumnType.MAP)) {
                tp.set(columnNum, generateMap(columnSchema.getSchema()));
            } else if (type.equals(ColumnType.RECORD)) {
                tp.set(columnNum, generateRecord(columnSchema.getSchema()));
            }
            columnNum++;
        }
        return tp;
    }

    public static Tuple transformTuple(Tuple tuple, OwlSchema originalSchema, OwlSchema outputSchema) throws ExecException, OwlException {

        if ((originalSchema == null) || (originalSchema.getSchemaString().equalsIgnoreCase(outputSchema.getSchemaString()))){
            return tuple;
        }

        Tuple txTuple = TupleFactory.getInstance().newTuple(outputSchema.getColumnCount());

        List<OwlColumnSchema> originalColumns = originalSchema.getColumnSchema();
        List<OwlColumnSchema> outputColumns = outputSchema.getColumnSchema();

        Map<String,OwlColumnSchema> originalColumnsByName = new HashMap<String,OwlColumnSchema>();
        Map<OwlColumnSchema,Integer> columnPositionByColumn = new HashMap<OwlColumnSchema,Integer>();

        int columnNum = 0;
        for (OwlColumnSchema col : originalColumns){
            //FIXME:.getName() can return null in the case of anonymous record field in a collection or map
            originalColumnsByName.put(col.getName(), col);
            columnPositionByColumn.put(col,columnNum);
            columnNum++;
        }

        columnNum = 0;
        for (OwlColumnSchema col : outputColumns){
            if (originalColumnsByName.containsKey(col.getName())){
                OwlColumnSchema originalColumn = originalColumnsByName.get(col.getName());
                ColumnType type = col.getType();
                if (type != originalColumn.getType()){
                    throw new OwlException(
                            ErrorType.ZEBRA_SCHEMA_EXCEPTION,
                            "Type mismatch, cannot transform type [" + type + 
                            "] to type [" + originalColumn.getType() + "]"
                    );
                }
                if (type.equals(ColumnType.COLLECTION)) {
                    txTuple.set(
                            columnNum,
                            _transformCollection(
                                    tuple.get(columnPositionByColumn.get(originalColumn)),
                                    col.getSchema(),originalColumn.getSchema()
                            )
                    );
                } else if (type.equals(ColumnType.MAP)) {
                    txTuple.set(
                            columnNum,
                            _transformMap(
                                    tuple.get(columnPositionByColumn.get(originalColumn)),
                                    col.getSchema(),originalColumn.getSchema()
                            )
                    );
                } else if (type.equals(ColumnType.RECORD)) {
                    txTuple.set(
                            columnNum,
                            _transformRecord(
                                    tuple.get(columnPositionByColumn.get(originalColumn)),
                                    col.getSchema(),originalColumn.getSchema()
                            )
                    );
                } else {
                    txTuple.set(
                            columnNum,
                            tuple.get(columnPositionByColumn.get(originalColumn))
                    );
                }
            }else{
                txTuple.set(columnNum,null);
            }
            columnNum++;
        }
        return txTuple;
    }

    public static Object generateRecord(OwlSchema schema) throws ExecException, OwlException {
        return generateTuple(schema);
    }

    private static Object _transformRecord(Object object, OwlSchema originalSchema,
            OwlSchema outputSchema) throws ExecException, OwlException {
        return transformTuple((Tuple) object, originalSchema, outputSchema);
    }

    public static Object generateCollection(OwlSchema schema) throws ExecException, OwlException {
        DataBag bag = new DefaultDataBag();
        // randomly generate 1 to MAX_TUPLES_IN_BAG tuples in the bag
        for (int i = 0; i < (1 + (RandomUtils.nextInt() % MAX_TUPLES_IN_BAG) ) ; i++){
            bag.add(generateTuple(schema));
        }
        return bag;
    }

    private static Object _transformCollection(Object object,
            OwlSchema originalSchema, OwlSchema outputSchema) throws ExecException, OwlException {
        if ((originalSchema == null) || (originalSchema.getSchemaString().equalsIgnoreCase(outputSchema.getSchemaString()))){
            return object;
        }
        DataBag db = (DataBag) object;
        DataBag outputBag = new DefaultDataBag();
        Iterator<Tuple> dbi = db.iterator();
        while (dbi.hasNext()){
            outputBag.add(transformTuple(dbi.next(), originalSchema, outputSchema));
        }
        return outputBag;
    }

    public static Object generateMap(OwlSchema schema) throws ExecException, OwlException {
        Map<String,Object> map = new HashMap<String,Object>();
        for (int i = 0; i < (1 + (RandomUtils.nextInt() % MAX_ENTRIES_IN_MAP) ) ; i++){
            map.put((String) generateString(),generateTuple(schema).get(0));
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static Object _transformMap(Object object,
            OwlSchema originalSchema, OwlSchema outputSchema) throws ExecException, OwlException {
        if ((originalSchema == null) || (originalSchema.getSchemaString().equalsIgnoreCase(outputSchema.getSchemaString()))){
            return object;
        }
        Map<String,Object> map = (Map<String,Object>)object;
        Map<String,Object> outputMap = new HashMap<String,Object>();
        for (String key : map.keySet()){
            map.put(key, transformTuple((Tuple) map.get(key),originalSchema,outputSchema));
        }
        return outputMap;
    }

    public static Object generateString() {
        return "S" + RandomUtils.nextInt() + "S" ;
    }

    public static Object generateLong() {
        return RandomUtils.nextLong();
    }

    public static Object generateInt() {
        return RandomUtils.nextInt();
    }

    public static Object generateFloat() {
        return RandomUtils.nextFloat();
    }

    public static Object generateDouble() {
        return RandomUtils.nextDouble();
    }

    public static Object generateBytes() {
        return new DataByteArray("B"+RandomUtils.nextInt()+"B");
    }

    public static Object generateBool() {
        return RandomUtils.nextBoolean();
    }

    /**
     * @param tuples the tuples to set
     */
    public void setTuples(List<Tuple> tuples) {
        this.tuples = tuples;
    }

    /**
     * @return the tuples
     */
    public List<Tuple> getTuples() {
        return tuples;
    }

    /**
     * @param numPartFiles the numPartFiles to set
     */
    public void setNumPartFiles(int numPartFiles) {
        this.numPartFiles = numPartFiles;
    }

    /**
     * @return the numPartFiles
     */
    public int getNumPartFiles() {
        return numPartFiles;
    }

    /**
     * @param keyPrefix the keyPrefix to set
     */
    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    /**
     * @return the keyPrefix
     */
    public String getKeyPrefix() {
        return keyPrefix;
    }

    /**
     * @param schema the schema to set
     */
    public void setSchema(OwlSchema schema) {
        this.schema = schema;
    }

    /**
     * @return the schema
     */
    public OwlSchema getSchema() {
        return schema;
    }

    /**
     * @param path the path to set
     */
    public void setPath(Path path) {
        this.path = path;
    }

    /**
     * @return the path
     */
    public Path getPath() {
        return path;
    }

    public String getSerializedJSONMetaInfo() throws OwlException {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        sb.append("\"schema\" : ");
        sb.append(OwlUtil.getJSONFromObject(schema));
        sb.append(",");
        sb.append("\"keyPrefix\" : \"");
        sb.append(keyPrefix);
        sb.append("\",");
        sb.append("\"numPartFiles\" : ");
        sb.append(new Integer(numPartFiles));
        sb.append(",");
        sb.append("\"numRows\" : ");
        sb.append(new Integer(tuples.size()));
        sb.append(",");
        sb.append("\"path\" : \"");
        sb.append(path.toString());
        sb.append("\" }");
        return sb.toString();
    }

    public SampleDataGenerator(String serializedJSONMetaInfo, List<Tuple> tuples) throws ExecException, OwlException {

        JSONObject jso = JSONObject.fromObject(serializedJSONMetaInfo);

        OwlSchema schema = (OwlSchema) OwlUtil.getObjectFromJSON( OwlSchema.class, null, jso.get("schema").toString());
        this.setSchema(schema);

        this.setKeyPrefix((String) jso.get("keyPrefix"));
        this.setNumPartFiles( jso.getInt("numPartFiles") );
        this.setPath(new Path((String) jso.get("path")));

        if (tuples != null){
            this.setTuples(tuples);
        }else{
            int numRows = jso.getInt("numRows");
            this.setTuples(generateTuples(numRows,schema));
        }

    }

}
