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

package org.apache.hadoop.zebra.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.Utils;
import org.apache.pig.data.DataType;

public enum ColumnType {
  ANY("any") {
	  public byte pigDataType() {
		  return DataType.UNKNOWN;
	  }
  },
  INT("int") {
    public byte pigDataType() {
      return DataType.INTEGER;
    }
  },
  LONG("long") {
    public byte pigDataType() {
      return DataType.LONG;
    }
  },
  FLOAT("float") {
    public byte pigDataType() {
      return DataType.FLOAT;
    }
  },
  DOUBLE("double") {
    public byte pigDataType() {
      return DataType.DOUBLE;
    }
  },
  BOOL("bool") {
    public byte pigDataType() {
      return DataType.BOOLEAN;
    }
  },
  COLLECTION("collection") {
    public byte pigDataType() {
      return DataType.BAG;
    }
  },
  MAP("map") {
    public byte pigDataType() {
      return DataType.MAP;
    }
  },
  RECORD("record") {
    public byte pigDataType() {
      return DataType.TUPLE;
    }
  },
  STRING("string") {
    public byte pigDataType() {
      return DataType.CHARARRAY;
    }
  },
  BYTES("bytes") {
    public byte pigDataType() {
      return DataType.BYTEARRAY;
    }
  };

  private String name;

  private ColumnType(String name) {
    this.name = name;
  }

  public abstract byte pigDataType();

  /**
   * To get the type based on the type name string.
   * 
   * @param name name of the type
   * 
   * @return ColumnType Enum for the type
   */
  public static ColumnType getTypeByName(String name) {
    return ColumnType.valueOf(name.toUpperCase());
  }

  public static ColumnType getTypeByPigDataType(byte dt) {
    for (ColumnType ct : ColumnType.values()) {
      if (ct.pigDataType() == dt) {
        return ct;
      }
    }
    return null;
  }

  public static String findTypeName(ColumnType columntype) {
	  return columntype.getName();
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return name;
  }

  public static boolean isSchemaType(ColumnType columnType) {
	  return ((columnType == RECORD) || (columnType == MAP) || (columnType == COLLECTION));
  }
}
