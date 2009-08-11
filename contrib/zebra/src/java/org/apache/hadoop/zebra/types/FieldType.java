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

package org.apache.hadoop.zebra.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.Utils;
import org.apache.pig.data.DataType;

public enum FieldType implements Writable {
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

    public boolean isNested() {
      return true;
    }

    public int getMaxNumsNestedField() {
      return MAX_NUM_FIELDS;
    }
  },

  MAP("map") {
    public byte pigDataType() {
      return DataType.MAP;
    }

    public boolean isNested() {
      return true;
    }

    public int getMaxNumsNestedField() {
      // map can only contain one field for values
      return 1;
    }
  },

  RECORD("record") {
    public byte pigDataType() {
      return DataType.TUPLE;
    }

    public boolean isNested() {
      return true;
    }

    public int getMaxNumsNestedField() {
      return MAX_NUM_FIELDS;
    }
  },

  STRING("string") {
    public byte pigDataType() {
      return DataType.CHARARRAY;
    }
  },

  BYTES("bytes") {
    public byte pigDataType() {
      return DataType.BIGCHARARRAY;
    }
  };

  public /*final*/ static int MAX_NUM_FIELDS = 4096;

  private static Map<Byte, FieldType> mapPigDT2This =
      new HashMap<Byte, FieldType>(FieldType.values().length);

  private String name;

  static {
    for (FieldType t : FieldType.values()) {
      mapPigDT2This.put(t.pigDataType(), t);
    }
  }

  private FieldType(String name) {
    this.name = name;
  }

  public abstract byte pigDataType();

  /**
   * To get the type based on the name of the type.
   * 
   * @param name name fo the type
   * 
   * @return FieldType Enum for the type
   */
  public static FieldType getTypeByName(String name) {
    return FieldType.valueOf(name.toUpperCase());
  }

  public static FieldType getTypeByPigDataType(byte dt) {
    return mapPigDT2This.get(dt);
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return name;
  }

  public boolean isNested() {
    return false;
  }

  public int getMaxNumsNestedField() {
    return 0;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // no op, instantiated by the caller
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Utils.writeString(out, name);
  }
}
