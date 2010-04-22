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

package org.apache.hadoop.zebra.types;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.*;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.zebra.tfile.Utils.Version;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.parser.ParseException;

  /**
   * ColumnGroup Schema. This object is first written to a schema file when the
   * ColumnGroup is initially created, and is used to communicate meta
   * information among writers.
   */
public class CGSchema {
	private Version version;
   private boolean sorted;
   private String comparator;
   private Schema schema;
   private String name = null;
   private String compressor = "lzo";
   private String serializer = "pig";
   private String group		 = null;
   private String owner		 = null;
   private short  perm		 = -1;
   
   // tmp schema file name, used as a flag of unfinished CG
   private final static String SCHEMA_FILE = ".schema";
	// schema version, should be same as BasicTable's most of the time
   private final static Version SCHEMA_VERSION =
     new Version((short) 1, (short) 1);

   @Override
   public String toString() {
     StringBuilder sb = new StringBuilder();
     sb.append("{Name = ");
     sb.append(name);
     sb.append("}\n");
     sb.append("{Compressor = ");
     sb.append(compressor);
     sb.append("}\n");
     sb.append("{Serializer = ");
     sb.append(serializer);
     sb.append("}\n");
     sb.append(schema.toString());
     
     return sb.toString();
   }
   
   public static Path makeFilePath(Path parent) {
     return new Path(parent, SCHEMA_FILE);
   }

   public static CGSchema load(FileSystem fs, Path parent) throws IOException, ParseException {
     if (!exists(fs, parent)) return null;
     
     CGSchema ret = new CGSchema();
     ret.read(fs, parent);
     return ret;
   }
      
   public CGSchema() {
     this.version = SCHEMA_VERSION;
   }

   public CGSchema(Schema schema, boolean sorted, String comparator) {
     this.sorted = sorted;
     this.comparator = (sorted) ? (comparator == null ? SortInfo.DEFAULT_COMPARATOR : comparator) : "";
     this.schema = schema;
     this.version = SCHEMA_VERSION;
   }

   public CGSchema(Schema schema, boolean sorted, String comparator, String name, String serializer, String compressor, String owner, String group, short perm) {
  	this(schema, sorted, comparator);
    this.name = name;
  	this.serializer = serializer;
  	this.compressor = compressor;
  	this.group = group;
  	this.perm  = perm;
   }

   @Override
   public boolean equals(Object obj) {
     CGSchema other = (CGSchema) obj;
     return this.sorted == other.sorted
         && this.comparator.equals(other.comparator)
         && this.schema.equals(other.schema);
   }

   public static boolean exists(FileSystem fs, Path parent) {
     try {
       return fs.exists(makeFilePath(parent));
     }
     catch (IOException e) {
       return false;
     }
   }

   public static void drop(FileSystem fs, Path parent) throws IOException {
     fs.delete(makeFilePath(parent), true);
   }

   public boolean isSorted() {
     return sorted;
   }

   public String getComparator() {
     return comparator;
   }

   public String getName() {
     return name;
   }
   
   public void setName(String newName) {
     name = newName;
   }

   public String getSerializer() {
     return serializer;
   }

   public String getCompressor() {
     return compressor;
   }

   public String getGroup() {
	     return this.group;
   }

   public short getPerm() {
	     return this.perm;
   }

   public String getOwner() {
	     return this.owner;
   } 
   
   
   
   public void create(FileSystem fs, Path parent) throws IOException {
	 fs.mkdirs(parent);
	 if(this.owner != null || this.group != null) {
	   fs.setOwner(parent, this.owner, this.group);
	 }  

	 if(this.perm != -1) {
       fs.setPermission(parent, new FsPermission((short) this.perm));

	 }
	 
	 FSDataOutputStream outSchema = fs.create(makeFilePath(parent), false);
	 if(this.owner != null || this.group != null) {
	   fs.setOwner(makeFilePath(parent), owner, group);
	 }  
	 if(this.perm != -1) {
       fs.setPermission(makeFilePath(parent), new FsPermission((short) this.perm));

	 }
     version.write(outSchema);
       
	 WritableUtils.writeString(outSchema, schema.toString());
     WritableUtils.writeVInt(outSchema, sorted ? 1 : 0);
     WritableUtils.writeString(outSchema, comparator);
	 WritableUtils.writeString(outSchema, this.getCompressor());
	 WritableUtils.writeString(outSchema, this.getSerializer());
	 WritableUtils.writeString(outSchema, this.getGroup());
     WritableUtils.writeVInt(outSchema, this.getPerm());
     WritableUtils.writeString(outSchema, name);

     outSchema.close();
   }

 public  void read(FileSystem fs, Path parent) throws IOException, ParseException {
     FSDataInputStream in = fs.open(makeFilePath(parent));
	   version = new Version(in);
     // verify compatibility against SCHEMA_VERSION
     if (!version.compatibleWith(SCHEMA_VERSION)) {
       new IOException("Incompatible versions, expecting: " + SCHEMA_VERSION
            + "; found in file: " + version);
     }     
     
     name = parent.getName();     
     String s = WritableUtils.readString(in);
     schema = new Schema(s);
     sorted = WritableUtils.readVInt(in) == 1 ? true : false;
     comparator = WritableUtils.readString(in);
     // V2 table;
     if(version.compareTo(SCHEMA_VERSION) >= 0) { 
       compressor = WritableUtils.readString(in);
       serializer = WritableUtils.readString(in);  
   	   owner		= null;
       group 		= WritableUtils.readString(in);
       perm 		= (short) WritableUtils.readVInt(in);
       name = WritableUtils.readString(in);
     }  
     in.close();
   }

   public Schema getSchema() {
     return schema;
   }
 }
