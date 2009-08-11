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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.file.tfile.Utils.Version;
import org.apache.hadoop.zebra.types.Schema;

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
   private String compressor = "lzo2";
   private String serializer = "pig";
   // tmp schema file name, used as a flag of unfinished CG
   private final static String SCHEMA_FILE = ".schema";
   private final static String DEFAULT_COMPARATOR = "memcmp";
	// schema version, should be same as BasicTable's most of the time
   private final static Version SCHEMA_VERSION =
     new Version((short) 1, (short) 0);

   @Override
   public String toString() {
     StringBuilder sb = new StringBuilder();
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

   public CGSchema(Schema schema, boolean sorted) {
     this.sorted = sorted;
     this.comparator = (sorted) ? DEFAULT_COMPARATOR : "";
     this.schema = schema;
     this.version = SCHEMA_VERSION;
   }

   public CGSchema(Schema schema, boolean sorted, String serializer, String compressor) {
  	this(schema, sorted);
  	this.serializer = serializer;
  	this.compressor = compressor;
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

   public String getSerializer() {
     return serializer;
   }

   public String getCompressor() {
     return compressor;
   }

   public void create(FileSystem fs, Path parent) throws IOException {
     FSDataOutputStream outSchema = fs.create(makeFilePath(parent), false);
	  version.write(outSchema);
     WritableUtils.writeString(outSchema, schema.toString());
     WritableUtils.writeVInt(outSchema, sorted ? 1 : 0);
     WritableUtils.writeString(outSchema, comparator);
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
     String s = WritableUtils.readString(in);
     schema = new Schema(s);
     sorted = WritableUtils.readVInt(in) == 1 ? true : false;
     comparator = WritableUtils.readString(in);
     in.close();
   }

   public Schema getSchema() {
     return schema;
   }
 }
