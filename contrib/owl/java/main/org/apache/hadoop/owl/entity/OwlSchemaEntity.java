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
package org.apache.hadoop.owl.entity;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.backend.OwlSchemaBackend;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.schema.ParseException;
import org.apache.hadoop.owl.schema.Schema;
import org.apache.hadoop.owl.schema.Schema.ColumnSchema;

/** The entity representing a schema in the database*/
public class OwlSchemaEntity extends OwlResourceEntity {


    private int    id;

    private String owner;

    private String description;

    private long   createdAt;

    private long   lastModifiedAt;

    /** The schema version. */
    private int    version;

    /** The list of OwlColumnSchema. */
    private ArrayList<OwlColumnSchemaEntity> owlColumnSchema = new ArrayList<OwlColumnSchemaEntity>();

    public OwlSchemaEntity(){
    }

    public OwlSchemaEntity(int id,
            String owner,
            String description,
            long createAt,
            long lastModifiedAt,
            int version,
            String schemaString,
            ArrayList<OwlColumnSchemaEntity> owlColumnSchema){
        this.id = id;
        this.owner = owner;
        this.description = description;
        this.createdAt = createAt;
        this.lastModifiedAt = lastModifiedAt;
        this.version = version;
        //this.schemaString = schemaString;
        // this.owlColumnSchema = owlColumnSchema;
        this.setOwlColumnSchema(owlColumnSchema);
    }

    /**
     * Instantiates a new schema entity from a schema object.
     * @param schema the schema to create
     * @throws OwlException 
     */
    public OwlSchemaEntity(Schema schema) throws OwlException{
        int columnNumber = 0;
        for (ColumnSchema cs : schema.getColumnSchemas()){
            // convert each ColoumnSchmea to owlColumnSchmeaEntity
            OwlColumnSchemaEntity cse = new OwlColumnSchemaEntity(cs, this, columnNumber);
            columnNumber++;
            this.owlColumnSchema.add(cse);
        }
    }

    /**
     * Instantiates a new schema entity from a schema string.
     * @param schemaString the schemaString
     * @throws OwlException 
     */
    public OwlSchemaEntity(String schemaString) throws OwlException{
        // what we suppose to get from the client only contains string
        // we need to convert the string to columnSchemas
        // to construct Schema object from OwlSchema object
        int columnNumber = 0;
        try{
            Schema schema = new Schema(schemaString);

            for (ColumnSchema cs : schema.getColumnSchemas()){
                // convert each ColoumnSchema to owlColumnSchemaEntity
                OwlColumnSchemaEntity cse = new OwlColumnSchemaEntity(cs, this, columnNumber);
                columnNumber++;
                this.owlColumnSchema.add(cse);
            }

            //this.schemaString = schemaString;
        }catch(ParseException e){
            throw new OwlException(ErrorType.PARSE_EXCEPTION, e.toString());
        }
    }

    @Override
    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    @Override
    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getLastModifiedAt() {
        return lastModifiedAt;
    }

    @Override
    public void setLastModifiedAt(long lastModifiedAt) {
        this.lastModifiedAt = lastModifiedAt;
    }

    @Override
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    public ArrayList<OwlColumnSchemaEntity> getOwlColumnSchema() throws OwlException {
        for (OwlColumnSchemaEntity ocse : this.owlColumnSchema ){
            if(ocse.getColumnNumber() == null){
                throw new OwlException(ErrorType.ERROR_NO_COLUMNNUMBER_OF_COLUMNSCHEMAENTITY, 
                        "column name ["+ (owlColumnSchema.get(0)).getName() +"]");
            }
        }
        return owlColumnSchema;
    }

    public void setOwlColumnSchema(ArrayList<OwlColumnSchemaEntity> owlColumnSchema){
        // set each column's columnNumber
        int index = 0;
        if((owlColumnSchema.get(0)).getColumnNumber() == null){
            // owlColumnSchema does not have columnNumber, we set
            for (OwlColumnSchemaEntity ocse : owlColumnSchema){
                ocse.setColumnNumber(index);
                index++;
            }
        }
        this.owlColumnSchema = owlColumnSchema;
    }

    /**
     * Fix up schema, updates the transient subschemas entity object for each of the column schemas.
     * @param backend the backend
     * @throws OwlException the owl exception
     */
    public void fixupSchema(OwlSchemaBackend backend) throws OwlException {

        for(OwlColumnSchemaEntity columnEntity : getOwlColumnSchema() ) {
            if( columnEntity.getSubSchemaId() != null ) {
                fixupColumnSchema(columnEntity, backend);
            }
        }
    }

    /**
     * Fix up column schema, called recursively to fixup a nested schema structure.
     * @param column the current column schema
     * @param backend the backend
     * @throws OwlException the owl exception
     */
    private void fixupColumnSchema(OwlColumnSchemaEntity column, OwlSchemaBackend backend) throws OwlException {
        OwlSchemaEntity schema = column.getSubSchema();

        if( schema == null ) {
            schema = backend.find("id = " + column.getSubSchemaId()).get(0);
            column.setSubSchema(schema);
        }

        for(OwlColumnSchemaEntity columnEntity : schema.getOwlColumnSchema() ) {
            if( columnEntity.getSubSchemaId() != null ) {
                fixupColumnSchema(columnEntity, backend);
            }
        }        
    }

    //    /**
    //     * Sort the list of OwlClumnSchema based on the position number of each column
    //     */
    //    private void sortColumnSchema(List<OwlColumnSchemaEntity> owlColumnSchemaEntityList){
    //        Collections.sort(owlColumnSchemaEntityList, new OwlColumnSchemaComparable());
    //    }
    //
    //    public static class OwlColumnSchemaComparable implements Comparator<OwlColumnSchemaEntity>{
    //        //@Override
    //        public int compare(OwlColumnSchemaEntity c1, OwlColumnSchemaEntity c2) {
    //            return (c1.getColumnNumber() < c2.getColumnNumber() ? -1 : (c1.getColumnNumber()== c2.getColumnNumber() ? 0 : 1));
    //        }
    //    }

    /**
     * Gets the string representation for the OwlSchemaEntity.
     * @param backend OwlBackend object to find subschema if the schema refers to other objects in the backend
     * @return the schema string
     * @throws OwlException the owl exception
     */
    public String getSchemaString(OwlBackend backend) throws OwlException {
        StringBuilder sb = new StringBuilder();
        //  if( schema == null ) {
        //      return null;
        //  }

        buildSchemaString(sb, this, ColumnType.COLLECTION, true, backend);
        return sb.toString();
    }

    /**
     * Gets the string representation for the OwlSchema.
     * @param sb the string buffer to append to
     * @param schema the schema
     * @param type the type
     * @param top if this the top level schema
     * @return the schema string
     * @throws OwlException the owl exception
     */
    private static void buildSchemaString(StringBuilder sb, OwlSchemaEntity schema, ColumnType type, boolean top, OwlBackend backend) throws OwlException {

        if( type == ColumnType.RECORD || type == ColumnType.COLLECTION ) {
            if( schema == null ) {
                throw new OwlException(ErrorType.ERROR_MISSING_SUBSCHEMA);
            }
        }

        if( schema == null ) {
            return;
        }

        if (!top) {
            if (type == ColumnType.RECORD) {
                sb.append("(");
            }
            else if (type == ColumnType.COLLECTION) {
                sb.append("(");
            }
            else if (type == ColumnType.MAP) {
                sb.append("(");
            }
        }

        boolean isFirst = true;

        int count = schema.owlColumnSchema.size(); //schema.getColumnCount();
        for (int i = 0; i < count; i++) {

            if (!isFirst) {
                sb.append(",");
            }
            else {
                isFirst = false;
            }

            OwlColumnSchemaEntity fs = schema.owlColumnSchema.get(i);//schema.getColumn(i);

            if (fs == null) {
                continue;
            }

            if (fs.getName() != null && !fs.getName().equals("")) {
                sb.append(fs.getName());
                sb.append(":");
            }

            sb.append(fs.getColumnType());
            //if ((fs.getType() == ColumnType.RECORD) || (fs.getType() == ColumnType.MAP)

            if (
                    (fs.getColumnType().equalsIgnoreCase(ColumnType.RECORD.getName()))
                    || (fs.getColumnType().equalsIgnoreCase(ColumnType.MAP.getName()))
                    || (fs.getColumnType().equalsIgnoreCase(ColumnType.COLLECTION.getName()))
            ) {
                // safety net
                // before using subSchema, fixSchema
                if( fs.getSubSchema() == null ) {
                    OwlSchemaEntity subSchema = (OwlSchemaEntity) backend.find(OwlSchemaEntity.class,"id = " + fs.getSubSchemaId()).get(0);
                    fs.setSubSchema(subSchema);
                    // by now, my subSchema of the columnSchema should be a real OwlSchemaEntity
                }
                if (schema != fs.getSubSchema()) {
                    buildSchemaString(sb, fs.getSubSchema(), ColumnType.getTypeByName(fs.getColumnType()), false, backend);
                }
                else {
                    throw new OwlException(ErrorType.INVALID_OWL_SCHEMA, "Schema refers to itself as inner schema");
                }
            }
        }

        if (!top) {
            if (type == ColumnType.RECORD) {
                sb.append(")");
            }
            else if (type == ColumnType.COLLECTION) {
                sb.append(")");
            }
            else if (type == ColumnType.MAP) {
                sb.append(")");
            }
        }
    }

}