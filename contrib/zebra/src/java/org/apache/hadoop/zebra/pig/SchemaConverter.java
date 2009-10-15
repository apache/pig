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
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * A simple schema converter that only understands three field types.
 */
class SchemaConverter {
  enum FieldSchemaMaker {
    SimpleField("SF_") {
      @Override
      FieldSchema toFieldSchema(String name) {
        return new FieldSchema(name, DataType.BYTEARRAY);
      }
    },

    MapField("MF_") {
      @Override
      FieldSchema toFieldSchema(String name) {
        // TODO: how to convey key and value types?
        return new FieldSchema(name, DataType.MAP);
      }
    },

    MapListField("MLF_") {
      @Override
      FieldSchema toFieldSchema(String name) throws FrontendException {
        Schema tupleSchema = new Schema();
        tupleSchema.add(MapField.toFieldSchema(null));
        tupleSchema.setTwoLevelAccessRequired(true);
        return new FieldSchema(name, tupleSchema, DataType.BAG);
      }
    };

    private String prefix;

    FieldSchemaMaker(String prefix) {
      this.prefix = prefix;
    }

    abstract FieldSchema toFieldSchema(String name) throws FrontendException;

    public static FieldSchema makeFieldSchema(String colname)
        throws FrontendException {
      for (FieldSchemaMaker e : FieldSchemaMaker.values()) {
        if (colname.startsWith(e.prefix)) {
          return e.toFieldSchema(colname.substring(e.prefix.length()));
        }
      }
      throw new FrontendException("Cannot determine type from column name");
    }
    
    public static String makeColumnName(FieldSchema fs)
        throws FrontendException {
      if (fs.alias == null) {
        throw new FrontendException("No alias provided for field schema");
      }
      for (FieldSchemaMaker e : FieldSchemaMaker.values()) {
        FieldSchema expected = e.toFieldSchema("dummy");
        if (FieldSchema.equals(fs, expected, false, true)) {
          return e.prefix + fs.alias;
        }
      }
      throw new FrontendException("Unsupported field schema");
    }
  }
  
  public static Schema toPigSchema(
      org.apache.hadoop.zebra.schema.Schema tschema)
      throws FrontendException {
    Schema ret = new Schema();
    for (String col : tschema.getColumns()) {
    	org.apache.hadoop.zebra.schema.Schema.ColumnSchema columnSchema = 
    		tschema.getColumn(col);
			if (columnSchema != null) {
        ret.add(new FieldSchema(col, columnSchema.getType().pigDataType()));
			} else {
				ret.add(new FieldSchema(null, null));
			}
    }
    
    return ret;
  }

  public static org.apache.hadoop.zebra.schema.Schema fromPigSchema(
      Schema pschema) throws FrontendException, ParseException {
    String[] colnames = new String[pschema.size()];
    int i = 0;
    for (Iterator<FieldSchema> it = pschema.getFields().iterator(); it
        .hasNext(); ++i) {
      FieldSchema fs = it.next();
      colnames[i] = FieldSchemaMaker.makeColumnName(fs);
    }
    return new org.apache.hadoop.zebra.schema.Schema(colnames);
  }
}
