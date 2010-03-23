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
package org.apache.hadoop.zebra.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.StringReader;
import java.util.HashSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.parser.TableSchemaParser;

/**
 * Logical schema of tabular data.
 */
public class Schema implements Comparable<Schema>, Writable {
  public final static String COLUMN_DELIMITER = ",";

  private static final long schemaVersion = 1L;

  /**
   * Column Schema in Schema
   */
  public static class ColumnSchema {
    private String name;
    private ColumnType type;
    private Schema schema;
    private int index; // field index in schema

    /**
     * construct a ColumnSchema for a native type
     * 
     * @param a
     *          column name
     * @param t
     *          native column type
     */
    public ColumnSchema(String a, ColumnType t) {
      name = a;
      type = t;
      schema = null;
    }

    /**
     * construct a Column schema for a RECORD column type
     * 
     * @param a
     *          column name
     * @param s
     *          column schema
     */
    public ColumnSchema(String a, Schema s) {
      name = a;
      type = ColumnType.RECORD;
      schema = s;
    }

    /**
     * access function to get the column name 
     *
     * @return name of the column
     */
    public String getName() {
      return name;
    }
    
    /**
     * access function to get the column type 
     *
     * @return column type
     */
    public ColumnType getType() {
      return type;
    }
    
    /**
     * access function to get the column name 
     *
     * @return column index in the parent schema
     */
    public int getIndex() {
      return index;
    }
    
    /**
     * construct a column schema for a complex column type
     * 
     * @param a
     *          column name
     * @param s
     *          column schema
     * @param t
     *          complex column type
     * @throws ParseException
     */
    public ColumnSchema(String a, Schema s, ColumnType t) throws ParseException {
      if ((null != s) && !(ColumnType.isSchemaType(t))) {
        throw new ParseException(
            "Only a COLLECTION or RECORD or MAP can have schemas.");
      }
      name = a;
      schema = s;
      type = t;
    }

    /**
     * copy ctor
     * 
     * @param cs
     *          source column schema
     */
    public ColumnSchema(ColumnSchema cs) {
      name = cs.name;
      type = cs.type;
      schema = cs.schema;
    }

    /**
     * Compare two field schema for equality
     * 
     * @param fschema one column schema to be compared
     * @param fother the other column schema to be compared
     * @return true if ColumnSchema are equal, false otherwise
     */
    public static boolean equals(ColumnSchema fschema, ColumnSchema fother) {
      if (fschema == null) {
        return false;
      }

      if (fother == null) {
        return false;
      }

      if (fschema.type != fother.type) {
        return false;
      }

      if ((fschema.name == null) && (fother.name == null)) {
        // good
      }
      else if ((fschema.name != null) && (fother.name == null)) {
        return false;
      }
      else if ((fschema.name == null) && (fother.name != null)) {
        return false;
      }
      else if (!fschema.name.equals(fother.name)) {
        return false;
      }

      if (ColumnType.isSchemaType(fschema.type)) {
        // Don't do the comparison if both embedded schemas are
        // null. That will cause Schema.equals to return false,
        // even though we want to view that as true.
        if (!(fschema.schema == null && fother.schema == null)) {
          // compare recursively using schema
          if (!fschema.schema.equals(fother.schema)) {
            return false;
          }
        }
      }
      return true;
    }

    /**
     * string representation of the schema
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      if (name != null) {
        sb.append(name);
      }
      sb.append(ColumnType.findTypeName(type));

      if (schema != null) {
        sb.append("(");
        sb.append(schema.toString());
        sb.append(")");
      }

      return sb.toString();
    }

    /*
     * Returns the schema for the next level structure, in record, collection
     * and map.
     *
     * @return Schema of the column
     */
    public Schema getSchema() {
      return schema;
    }
  }

  /**
   * Helper class to parse a column name string one section at a time and find
   * the required type for the parsed part.
   */
  public static class ParsedName {
    private String mName;
    private int mKeyOffset; // the offset where the keysstring starts
    private ColumnType mDT = ColumnType.ANY; // parent's type

    /**
     * Default ctor
     */
    public ParsedName() {
    }

    /**
     * Set the name
     *
     * @param name
     *            column name string
     */
    public void setName(String name) {
      mName = name;
    }

    /**
     * Set the name and type
     *
     * @param name
     *            column name string
     * @param pdt
     *            column type
     */
    public void setName(String name, ColumnType pdt) {
      mName = name;
      mDT = pdt;
    }

    void setName(String name, ColumnType pdt, int keyStrOffset) {
      this.setName(name, pdt);
      mKeyOffset = keyStrOffset;
    }

    /**
     * Set the column type
     *
     * @param dt
     *          column type to be set with
     */
    public void setDT(ColumnType dt) {
      mDT = dt;
    }

    /**
     * Get the column type
     * 
     * @return column type
     */
    public ColumnType getDT() {
      return mDT;
    }

    /**
     * Get the column name
     *
     * @return column name
     */
    public String getName() {
      return mName;
    }

    /**
     * Parse one sector of a fully qualified column name; also checks validity
     * of use of the MAP and RECORD delimiters
     *
     * @param fs
     *          column schema this column name is checked against with          
     */
    public String parseName(Schema.ColumnSchema fs) throws ParseException {
      int fieldIndex, hashIndex;
      fieldIndex = mName.indexOf('.');
      hashIndex = mName.indexOf('#');
      String prefix;
      if (hashIndex != -1 && fieldIndex == -1) {
        if (fs.type != ColumnType.MAP)
          throw new ParseException(mName + " : is not of type MAP");
        prefix = mName.substring(0, hashIndex);
        setName(mName.substring(hashIndex + 1), ColumnType.MAP);
      }
      else if (hashIndex == -1 && fieldIndex != -1) {
        if (fs.type != ColumnType.RECORD)
          throw new ParseException(mName + " : is not of type RECORD");
        prefix = mName.substring(0, fieldIndex);
        setName(mName.substring(fieldIndex + 1), ColumnType.RECORD);
      }
      else if (hashIndex != -1 && fieldIndex != -1) {
        if (hashIndex < fieldIndex) {
          if (fs.type != ColumnType.MAP)
            throw new ParseException(mName + " : is not of type MAP");
          prefix = mName.substring(0, hashIndex);
          setName(mName.substring(hashIndex + 1), ColumnType.MAP);
        }
        else {
          if (fs.type != ColumnType.RECORD)
            throw new ParseException(mName + " : is not of type RECORD");
          prefix = mName.substring(0, fieldIndex);
          setName(mName.substring(fieldIndex + 1), ColumnType.RECORD);
        }
      }
      else {
        prefix = mName; // no hash or subfield contained
        mDT = ColumnType.ANY;
      }
      return prefix;
    }
  }

  private ArrayList<ColumnSchema> mFields;
  private HashMap<String, ColumnSchema> mNames;
  private boolean projection;

  /**
   * Constructor - schema for empty schema (zero-column) .
   */
  public Schema() {
    init();
  }

  /**
   * Constructor - schema for empty projection/schema (zero-column) .
   *
   * @param projection
   *           A projection schema or not
   */
  public Schema(boolean projection) {
    this.projection = projection;
    init();
  }

  /**
   * Constructor - create a schema from a string representation.
   * 
   * @param schema
   *          A string representation of the schema. For this version, the
   *          schema string is simply a comma separated list of column names. Of
   *          course, comma (,) and space characters are illegal in column
   *          names. To maintain forward compatibility, please use only
   *          alpha-numeric characters in column names.
   */
  public Schema(String schema) throws ParseException {
    init(schema, false);
  }

  public Schema(String schema, boolean projection) throws ParseException {
    this.projection = projection;
    init(schema, projection);
  }

  public Schema(ColumnSchema fs) throws ParseException {
    init();
    add(fs);
  }

  /**
   * Constructor - create a schema from an array of column names.
   * 
   * @param columns
   *          An array of column names. To maintain forward compatibility,
   *          please use only alpha-numeric characters in column names.
   */
  public Schema(String[] columns) throws ParseException {
    init(columns, false);
  }

  /**
   * add a column
   * 
   * @param f
   *          Column to be added to the schema
   */
  public void add(ColumnSchema f) throws ParseException {
    if (f == null) {
      if (!projection)
        throw new ParseException("Empty column schema is not allowed");
      mFields.add(null);
      return;
    }
    f.index = mFields.size();
    mFields.add(f);
    if (null != f && null != f.name) {
      if (mNames.put(f.name, f) != null && !projection)
        throw new ParseException("Duplicate field name: " + f.name);
    }
  }

  /**
   * get a column by name
   */
  public ColumnSchema getColumn(String name) {
    return mNames.get(name);
  }

  /**
   * Get the names of the individual columns.
   * 
   * @return An array of the column names.
   */
  public String[] getColumns() {
    String[] result = new String[mFields.size()];
    for (int i = 0; i < mFields.size(); i++) {
      ColumnSchema cs = mFields.get(i);
      if (cs != null) {
        result[i] = mFields.get(i).name;
      }
      else {
        result[i] = null;
      }
    }
    return result;
  }

  /**
   * Get a particular column's schema
   */
  public ColumnSchema getColumn(int index) {
    return mFields.get(index);
  }

  public String getColumnName(int index) {
    if (mFields.get(index) == null) return null;
    return mFields.get(index).name;
  }

  /**
   * Get the names and types of the individual columns.
   * 
   * @return An array of the column names.
   */
  public String[] getTypedColumns() {
    String[] result = new String[mFields.size()];
    for (int i = 0; i < mFields.size(); i++)
      result[i] = mFields.get(i).name + ":" + mFields.get(i).toString();
    return result;
  }

  /**
   * Get the index of the column for the input column name.
   * 
   * @param name
   *          input column name.
   * @return The column index if the name is valid; -1 otherwise.
   */
  public int getColumnIndex(String name) {
    ColumnSchema fs;
    if ((fs = mNames.get(name)) != null) return fs.index;
    return -1;
  }

  /**
   * Get the number of columns as defined in the schema.
   * 
   * @return The number of columns as defined in the schema.
   */
  public int getNumColumns() {
    return mFields.size();
  }

  /**
   * Parse a schema string and create a schema object.
   * 
   * @param schema
   *          comma separated schema string.
   * @return Schema object
   */
  public static Schema parse(String schema) throws ParseException {
    Schema s = new Schema();
    s.init(schema, false);
    return s;
  }

  /**
   * Convert the schema to a String.
   * 
   * @return the string representation of the schema.
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    try {
      stringifySchema(sb, this, ColumnType.COLLECTION, true);
    }
    catch (Exception fee) {
      throw new RuntimeException("PROBLEM PRINTING SCHEMA");
    }
    return sb.toString();
  }

  /**
   * This is used for building up output string type can only be COLLECTION or
   * RECORD or MAP
   */
  private void stringifySchema(StringBuilder sb, Schema schema,
      ColumnType type, boolean top) {
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
    for (int i = 0; i < schema.getNumColumns(); i++) {

      if (!isFirst) {
        sb.append(",");
      }
      else {
        isFirst = false;
      }

      ColumnSchema fs = schema.getColumn(i);

      if (fs == null) {
        continue;
      }

      if (fs.name != null && !fs.name.equals("")) {
        sb.append(fs.name);
        sb.append(":");
      }

      sb.append(ColumnType.findTypeName(fs.type));
      if ((fs.type == ColumnType.RECORD) || (fs.type == ColumnType.MAP)
          || (fs.type == ColumnType.COLLECTION)) {
        // safety net
        if (this != fs.schema) {
          stringifySchema(sb, fs.schema, fs.type, false);
        }
        else {
          throw new IllegalArgumentException("Schema refers to itself "
              + "as inner schema");
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

  /**
   * Normalize the schema string.
   * 
   * @param value
   *          the input string representation of the schema.
   * @return the normalized string representation.
   */
  public static String normalize(String value) {
    String result = new String();

    if (value == null || value.trim().isEmpty()) return result;

    StringBuilder sb = new StringBuilder();
    String[] parts = value.trim().split(COLUMN_DELIMITER);
    for (int nx = 0; nx < parts.length; nx++) {
      if (nx > 0) sb.append(COLUMN_DELIMITER);
      sb.append(parts[nx].trim());
    }
    return sb.toString();
  }

  /**
   * @see Comparable#compareTo(Object)
   */
  @Override
  public int compareTo(Schema other) {
    int mFieldsSize = this.mFields.size(); 
    if (mFieldsSize != other.mFields.size()) {
      return mFieldsSize - other.mFields.size();
    }
    int ret = 0;
    for (int nx = 0; nx < mFieldsSize; nx++) {
      Schema mFieldSchema = mFields.get(nx).schema;
      Schema otherFieldSchema = other.mFields.get(nx).schema;
      if (mFieldSchema == null
          && otherFieldSchema != null) return -1;
      else if (mFieldSchema != null
          && otherFieldSchema == null) return 1;
      else if (mFieldSchema == null
          && otherFieldSchema == null) return 0;
      ret = mFieldSchema.compareTo(otherFieldSchema);
      if (ret != 0) {
        return ret;
      }
    }
    return 0;
  }

  /**
   * @see Object#equals(Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Schema other = (Schema) obj;

    if (mFields.size() != other.mFields.size()) return false;

    Iterator<ColumnSchema> i = mFields.iterator();
    Iterator<ColumnSchema> j = other.mFields.iterator();

    while (i.hasNext()) {

      ColumnSchema myFs = i.next();
      ColumnSchema otherFs = j.next();

      if ((myFs.name == null) && (otherFs.name == null)) {
        // good
      }
      else if ((myFs.name != null) && (otherFs.name == null)) {
        return false;
      }
      else if ((myFs.name == null) && (otherFs.name != null)) {
        return false;
      }
      else if (!myFs.name.equals(otherFs.name)) {
        return false;
      }

      if (myFs.type != otherFs.type) {
        return false;
      }

      // Compare recursively using field schema
      if (!ColumnSchema.equals(myFs, otherFs)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @see Writable#readFields(DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    long version = org.apache.hadoop.zebra.tfile.Utils.readVLong(in);

    if (version > schemaVersion)
      throw new IOException("Schema version is newer than that in software.");

    // check-ups are needed for future versions for backward-compatibility
    String strSchema = org.apache.hadoop.zebra.tfile.Utils.readString(in);
    try {
      init(strSchema, false);
    }
    catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * @see Writable#write(DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    org.apache.hadoop.zebra.tfile.Utils.writeVLong(out, schemaVersion);
    org.apache.hadoop.zebra.tfile.Utils.writeString(out, toString());
  }

  private void init(String[] columnNames, boolean projection) throws ParseException {
    // the arg must be of type or they will be treated as the default type
    mFields = new ArrayList<ColumnSchema>();
    mNames = new HashMap<String, ColumnSchema>();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < columnNames.length; i++) {
      if (columnNames[i].contains(COLUMN_DELIMITER))
        throw new ParseException("Column name should not contain "
            + COLUMN_DELIMITER);
      if (i > 0) sb.append(",");
      sb.append(columnNames[i]);
    }
    TableSchemaParser parser =
        new TableSchemaParser(new StringReader(sb.toString()));
    if (projection)
      parser.ProjectionSchema(this);
    else
      parser.RecordSchema(this);
  }

  private void init() {
    mFields = new ArrayList<ColumnSchema>();
    mNames = new HashMap<String, ColumnSchema>();
  }

  private void init(String columnString, boolean projection) throws ParseException {
    String trimmedColumnStr;
    if (columnString == null || (trimmedColumnStr = columnString.trim()).isEmpty()) {
      init();
      return;
    }

    String[] parts = trimmedColumnStr.split(COLUMN_DELIMITER);
    for (int nx = 0; nx < parts.length; nx++) {
      parts[nx] = parts[nx].trim();
    }
    init(parts, projection);
  }

  /**
   * Get a projection's schema
   */
  public Schema getProjectionSchema(String[] projcols,
      HashMap<Schema.ColumnSchema, HashSet<String>> keysmap)
      throws ParseException {
    int ncols = projcols.length;
    Schema result = new Schema(true);
    ColumnSchema cs, mycs;
    String keysStr;
    String[] keys;
    ParsedName pn = new ParsedName();
    HashSet<String> keyentries;
    for (int i = 0; i < ncols; i++) {
	    if (Projection.isVirtualColumn(projcols[i]))
	    {
	      mycs = 
	    	  new ColumnSchema(
	    		  Projection.source_table_vcolumn_name, null, ColumnType.INT
	    	  );	
	      result.add(mycs);
	      continue;
	    }
      pn.setName(projcols[i]);
      if ((cs = getColumnSchemaOnParsedName(pn)) != null) {
        mycs = new ColumnSchema(pn.mName, cs.schema, cs.type);
        result.add(mycs);
        if (pn.mDT == ColumnType.MAP) {
          keysStr = projcols[i].substring(pn.mKeyOffset);
          if (!keysStr.startsWith("{") || !keysStr.endsWith("}"))
            throw new ParseException("Invalid map key specification in "
                + projcols[i]);
          keysStr = keysStr.substring(1, keysStr.length() - 1);
          if ((keysStr.indexOf('{') != -1) || (keysStr.indexOf('}') != -1)
              || (keysStr.indexOf('.') != -1) || (keysStr.indexOf('#') != -1))
            throw new ParseException("Invalid map key specification in "
                + projcols[i]);
          keys = keysStr.split("\\|");
          if ((keyentries = keysmap.get(mycs)) == null) {
            keyentries = new HashSet<String>();
            keysmap.put(mycs, keyentries);
          }
          for (int j = 0; j < keys.length; j++) {
            keyentries.add(keys[j]);
          }
        }
      } else { 
    	  result.add( new ColumnSchema(pn.mName, null, ColumnType.ANY ) );
      }
    }
    return result;
  }

  /**
   * Get a column's schema
   *
   * @param name
   *          column name
   * @return Column schema for the named column
   */
  public ColumnSchema getColumnSchema(String name) throws ParseException {
    int hashIndex, fieldIndex;
    String currentName = name, prefix;
    Schema currentSchema = this;
    ColumnSchema fs = null;

    while (true) {
      /**
       * this loop is necessary because the top columns in schema may contain .s
       * and #s, particularly for column group schemas
       */
      fieldIndex = currentName.lastIndexOf('.');
      hashIndex = currentName.lastIndexOf('#');
      if (fieldIndex == -1 && hashIndex == -1) {
        break;
      }
      else if (hashIndex != -1 && (fieldIndex == -1 || hashIndex > fieldIndex)) {
        currentName = currentName.substring(0, hashIndex);
        if (getColumn(currentName) != null) break;
      }
      else {
        currentName = currentName.substring(0, fieldIndex);
        if (getColumn(currentName) != null) break;
      }
    }

    currentName = name;
    while (true) {
      if (fieldIndex == -1 && hashIndex == -1) {
        fs = currentSchema.getColumn(currentName);
        return fs;
      }
      else if (hashIndex != -1 && (fieldIndex == -1 || hashIndex < fieldIndex)) {
        prefix = currentName.substring(0, hashIndex);
        fs = currentSchema.getColumn(prefix);
        if (fs == null)
          throw new ParseException("Column " + name + " does not exist");
        currentSchema = fs.schema;
        if (fs.type != ColumnType.MAP)
          throw new ParseException(name + " is not of type MAP");
        fs = currentSchema.getColumn(0);
        return fs;
      }
      else {
        prefix = currentName.substring(0, fieldIndex);
        if ((fs = currentSchema.getColumn(prefix)) == null)
          throw new ParseException("Column " + name + " does not exist");
        currentSchema = fs.schema;
        if (fs.type != ColumnType.RECORD && fs.type != ColumnType.COLLECTION)
          throw new ParseException(name
              + " is not of type RECORD or COLLECTION");
        currentName = currentName.substring(fieldIndex + 1);
        if (currentName.length() == 0)
          throw new ParseException("Column " + name
              + " does not have field after the record field separator '.'");
      }
      fieldIndex = currentName.indexOf('.');
      hashIndex = currentName.indexOf('#');
    }
  }

  /**
   * Get a subcolumn's schema and move the name just parsed into the next subtype
   *
   * @param pn
   *           The name of subcolumn to be parsed. On return it contains the
   *           subcolumn at the next level after parsing
   *
   * @return   the discovered Column Schema for the subcolumn
   */
  public ColumnSchema getColumnSchemaOnParsedName(ParsedName pn)
      throws ParseException {
    int hashIndex, fieldIndex, offset = 0;
    String name = pn.mName;

    /**
     * strip of any possible type specs
     */
    String currentName = name, prefix;
    Schema currentSchema = this;
    ColumnSchema fs = null;

    while (true) {
      /**
       * this loop is necessary because the top columns in schema may contain .s
       * and #s, particularly for column group schemas
       */
      fieldIndex = currentName.lastIndexOf('.');
      hashIndex = currentName.lastIndexOf('#');
      if (fieldIndex == -1 && hashIndex == -1) {
        break;
      }
      else if (hashIndex != -1 && (fieldIndex == -1 || hashIndex > fieldIndex)) {
        currentName = currentName.substring(0, hashIndex);
        if ((fs = getColumn(currentName)) != null) {
          if (fs.type != ColumnType.MAP)
            throw new ParseException(name + " is not of type MAP");
          offset += hashIndex;
          pn.setName(name.substring(0, hashIndex), ColumnType.MAP,
              hashIndex + 1);
          return fs;
        }
      }
      else {
        currentName = currentName.substring(0, fieldIndex);
        if (getColumn(currentName) != null) break;
      }
    }

    currentName = name;
    ColumnType ct = ColumnType.ANY;

    while (true) {
      if (fieldIndex == -1 && hashIndex == -1) {
        offset += currentName.length();
        pn.setName(name.substring(0, offset), ct);
        fs = currentSchema.getColumn(currentName);
        return fs;
      }
      else if (hashIndex != -1 && (fieldIndex == -1 || hashIndex < fieldIndex)) {
        prefix = currentName.substring(0, hashIndex);
        fs = currentSchema.getColumn(prefix);
        if (fs == null)
          throw new ParseException("Column " + name + " does not exist");
        currentSchema = fs.schema;
        if (fs.type != ColumnType.MAP)
          throw new ParseException(name + " is not of type MAP");
        offset += hashIndex;
        pn.setName(name.substring(0, offset), ColumnType.MAP, offset + 1);
        return fs;
      }
      else {
        prefix = currentName.substring(0, fieldIndex);
        if ((fs = currentSchema.getColumn(prefix)) == null)
          throw new ParseException("Column " + name + " does not exist");
        currentSchema = fs.schema;
        if (fs.type != ColumnType.RECORD && fs.type != ColumnType.COLLECTION)
          throw new ParseException(name
              + " is not of type RECORD or COLLECTION");
        else if( fs.type == ColumnType.COLLECTION ) 
		  throw new ParseException( name + "Projection within COLLECTION is not supported" );
        currentName = currentName.substring(fieldIndex + 1);
        if (currentName.length() == 0)
          throw new ParseException("Column " + name
              + " does not have field after the record field separator '.'");
        offset += fieldIndex + 1;
        ct = fs.type;
      }
      fieldIndex = currentName.indexOf('.');
      hashIndex = currentName.indexOf('#');
    }
  }

  /**
   * find the most fitting subcolumn containing the name: the parsed name is set
   * after the field name plus any possible separator of '.' or '#'.
   *
   * This is used to help discover the most fitting column schema in multiple
   * CG schemas.
   *
   * For instance, if pn contains a name of r.r1.f11 and current schema has
   * r.r1:record(f11:int, f12), it will return f11's column schema, and pn
   * is set at "f12".
   *
   * @param pn
   *           The name of subcolumn to be parsed. On return it contains the
   *           subcolumn at the next level after parsing
   *
   * @return   the discovered Column Schema for the subcolumn
   */
  public ColumnSchema getColumnSchema(ParsedName pn) throws ParseException {
    int maxlen = 0, size = getNumColumns(), len;
    Schema.ColumnSchema fs = null;
    String name = pn.getName(), fname;
    boolean whole = false, record = false, map = false, tmprecord = false, tmpmap =
        false;
    for (int i = 0; i < size; i++) {
      fname = getColumnName(i);
      if ((whole = name.equals(fname))
          || (tmprecord = name.startsWith(fname + "."))
          || (tmpmap = name.startsWith(fname + "#"))) {
        len = fname.length();
        if (len > maxlen) {
          maxlen = len;
          record = tmprecord;
          map = tmpmap;
          fs = getColumn(i);
          if (whole) break;
        }
      }
    }
    if (fs != null) {
      name = name.substring(maxlen);
      if (record) {
        if (fs.type != ColumnType.RECORD && fs.type != ColumnType.COLLECTION)
          throw new ParseException(name
              + " is not of type RECORD or COLLECTION");
        name = name.substring(1);
        pn.setName(name, fs.type);
      }
      else if (map) {
        if (fs.type != ColumnType.MAP)
          throw new ParseException(name + " is not of type MAP");
        name = name.substring(1);
        pn.setName(name, ColumnType.MAP);
      }
      else pn.setName(name, ColumnType.ANY);
      return fs;
    }
    else return null;
  }

  /**
   * union compatible schemas. Exception will be thrown if a name appears in
   * multiple schemas but the types are different.
   */
  public void unionSchema(Schema other) throws ParseException {
    int size = other.getNumColumns();
    ColumnSchema fs, otherfs;
    for (int i = 0; i < size; i++) {
      otherfs = other.getColumn(i);
      if (otherfs == null) continue;
      fs = getColumn(otherfs.name);
      if (fs == null) add(otherfs);
      else {
        if (!ColumnSchema.equals(fs, otherfs))
          throw new ParseException("Different types of column " + fs.name
              + " in tables of a union");
      }
    }
  }

  /**
   * return untyped schema string for projection
   */
  public String toProjectionString() {
    String result = new String();
    ColumnSchema fs;
    for (int i = 0; i < mFields.size(); i++) {
      if (i > 0) result += ",";
      if ((fs = mFields.get(i)) != null) result += fs.name;
    }
    return result;
  }
}
