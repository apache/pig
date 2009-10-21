/**
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

package org.apache.hadoop.zebra.pig;

import java.util.Iterator;

import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.ColumnType;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

class SchemaConverter {
  public static ColumnType toTableType(byte ptype)
  {
    ColumnType ret;
    switch (ptype) {
      case DataType.INTEGER:
        ret = ColumnType.INT; 
        break;
      case DataType.LONG:
        ret = ColumnType.LONG; 
        break;
      case DataType.FLOAT:
        ret = ColumnType.FLOAT; 
        break;
      case DataType.DOUBLE:
        ret = ColumnType.DOUBLE; 
        break;
      case DataType.BOOLEAN:
        ret = ColumnType.BOOL; 
        break;
      case DataType.BAG:
        ret = ColumnType.COLLECTION; 
        break;
      case DataType.MAP:
        ret = ColumnType.MAP; 
        break;
      case DataType.TUPLE:
        ret = ColumnType.RECORD; 
        break;
      case DataType.CHARARRAY:
        ret = ColumnType.STRING; 
        break;
      case DataType.BYTEARRAY:
        ret = ColumnType.BYTES; 
        break;
      default:
        ret = null;
        break;
    }
    return ret;
  }
  
  public static Schema toPigSchema(
      org.apache.hadoop.zebra.schema.Schema tschema)
      throws FrontendException {
    Schema ret = new Schema();
    for (String col : tschema.getColumns()) {
    	org.apache.hadoop.zebra.schema.Schema.ColumnSchema columnSchema = 
    		tschema.getColumn(col);
			if (columnSchema != null) {
        ColumnType ct = columnSchema.getType();
        if (ct == org.apache.hadoop.zebra.schema.ColumnType.RECORD ||
            ct == org.apache.hadoop.zebra.schema.ColumnType.COLLECTION)
          ret.add(new FieldSchema(col, toPigSchema(columnSchema.getSchema()), ct.pigDataType()));
        else
          ret.add(new FieldSchema(col, ct.pigDataType()));
			} else {
				ret.add(new FieldSchema(null, null));
			}
    }
    return ret;
  }

  public static org.apache.hadoop.zebra.schema.Schema fromPigSchema(
      Schema pschema) throws FrontendException, ParseException {
    org.apache.hadoop.zebra.schema.Schema tschema = new org.apache.hadoop.zebra.schema.Schema();
    Schema.FieldSchema columnSchema;
    for (int i = 0; i < pschema.size(); i++) {
    	columnSchema = pschema.getField(i);
    	if (columnSchema != null) {
    		if (DataType.isSchemaType(columnSchema.type))
    			tschema.add(new org.apache.hadoop.zebra.schema.Schema.ColumnSchema(columnSchema.alias, 
    					fromPigSchema(columnSchema.schema), toTableType(columnSchema.type)));
    		else if (columnSchema.type == DataType.MAP)
    			tschema.add(new org.apache.hadoop.zebra.schema.Schema.ColumnSchema(columnSchema.alias, 
    					new org.apache.hadoop.zebra.schema.Schema(new org.apache.hadoop.zebra.schema.Schema.ColumnSchema(null, 
    							org.apache.hadoop.zebra.schema.ColumnType.BYTES)), toTableType(columnSchema.type)));
    		else
    			tschema.add(new org.apache.hadoop.zebra.schema.Schema.ColumnSchema(columnSchema.alias, toTableType(columnSchema.type)));
		  } else {
		  	tschema.add(new org.apache.hadoop.zebra.schema.Schema.ColumnSchema(null, ColumnType.ANY));
		  }
    }
    return tschema;
  }
}
