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

import java.util.Iterator;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 */
class CsvZebraTupleOutput {
  private StringBuilder sb;
  private boolean isFirst = true;
  protected static final Log LOG = LogFactory.getLog(CsvZebraTupleOutput.class);
  private static CsvZebraTupleOutput instance = null;

  String toCSVString(String s) {
    StringBuffer sb = new StringBuffer(s.length() + 1);
    sb.append('\'');
    int len = s.length();
    for (int i = 0; i < len; i++) {
      char c = s.charAt(i);
      switch (c) {
      case '\0':
        sb.append("%00");
        break;
      case '\n':
        sb.append("%0A");
        break;
      case '\r':
        sb.append("%0D");
        break;
      case ',':
        sb.append("%2C");
        break;
      case '}':
        sb.append("%7D");
        break;
      case '%':
        sb.append("%25");
        break;
      default:
        sb.append(c);
      }
    }
    return sb.toString();
  }

  String toCSVBuffer(DataByteArray buf) {
    StringBuffer sb = new StringBuffer("#");
    sb.append(buf.toString());
    return sb.toString();
  }

  void printCommaUnlessFirst() {
    if (!isFirst) {
      sb.append(",");
    }
    isFirst = false;
  }

  /** Creates a new instance of CsvZebraTupleOutput */
  private CsvZebraTupleOutput() {
    sb = new StringBuilder();
  }

  void reset() {
    sb.delete(0, sb.length());
    isFirst = true;
  }

  static CsvZebraTupleOutput createCsvZebraTupleOutput() {
    if (instance == null) {
      instance = new CsvZebraTupleOutput();
    } else {
      instance.reset();
    }

    return instance;
  }

  @Override
  public String toString() {
    if (sb != null) {
      return sb.toString();
    }
    return null;
  }

  void writeByte(byte b) {
    writeLong((long) b);
  }

  void writeBool(boolean b) {
    printCommaUnlessFirst();
    String val = b ? "T" : "F";
    sb.append(val);
  }

  void writeInt(int i) {
    writeLong((long) i);
  }

  void writeLong(long l) {
    printCommaUnlessFirst();
    sb.append(l);
  }

  void writeFloat(float f) {
    writeDouble((double) f);
  }

  void writeDouble(double d) {
    printCommaUnlessFirst();
    sb.append(d);
  }

  void writeString(String s) {
    printCommaUnlessFirst();
    sb.append(toCSVString(s));
  }

  void writeBuffer(DataByteArray buf) {
    printCommaUnlessFirst();
    sb.append(toCSVBuffer(buf));
  }

  void writeNull() {
    printCommaUnlessFirst();
  }

  void startTuple(Tuple r) {
  }

  void endTuple(Tuple r) {
    sb.append("\n");
    isFirst = true;
  }

  /**
   * Generate CSV-format string representations of Zebra tuples for Zebra
   * streaming use.
   * 
   * @param tuple
   * @return CSV format string representation of Tuple
   */
  @SuppressWarnings("unchecked")
  void writeTuple(Tuple r) {
    for (int i = 0; i < r.size(); i++) {
      try {
        Object d = r.get(i);

        if (d != null) {
          if (d instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) d;
            startMap(map);
            writeMap(map);
            endMap(map);
          } else if (d instanceof Tuple) {
            Tuple t = (Tuple) d;
            writeTuple(t);
          } else if (d instanceof DataBag) {
            DataBag bag = (DataBag) d;
            writeBag(bag);
          } else if (d instanceof Boolean) {
            writeBool((Boolean) d);
          } else if (d instanceof Byte) {
            writeByte((Byte) d);
          } else if (d instanceof Integer) {
            writeInt((Integer) d);
          } else if (d instanceof Long) {
            writeLong((Long) d);
          } else if (d instanceof Float) {
            writeFloat((Float) d);
          } else if (d instanceof Double) {
            writeDouble((Double) d);
          } else if (d instanceof String) {
            writeString((String) d);
          } else if (d instanceof DataByteArray) {
            writeBuffer((DataByteArray) d);
          } else {
            throw new ExecException("Unknown data type");
          }
        } else { // if d is null, write nothing except ','
          writeNull();
        }
      } catch (ExecException e) {
        e.printStackTrace();
        LOG.warn("Exception when CSV format Zebra tuple", e);
      }
    }
  }

  void startBag(DataBag bag) {
    printCommaUnlessFirst();
    sb.append("v{");
    isFirst = true;
  }

  void writeBag(DataBag bag) {
    Iterator<Tuple> iter = bag.iterator();
    while (iter.hasNext()) {
      Tuple t = (Tuple) iter.next();
      startTuple(t);
      writeTuple(t);
      endTuple(t);
    }
  }

  void endBag(DataBag bag) {
    sb.append("}");
    isFirst = false;
  }

  void startMap(Map<String, Object> m) {
    printCommaUnlessFirst();
    sb.append("m{");
    isFirst = true;
  }

  void endMap(Map<String, Object> m) {
    sb.append("}");
    isFirst = false;
  }

  @SuppressWarnings("unchecked")
  void writeMap(Map<String, Object> m) throws ExecException {
    for (Map.Entry<String, Object> e : m.entrySet()) {
      writeString(e.getKey());

      Object d = e.getValue();
      if (d != null) {
        if (d instanceof Map) {
          Map<String, Object> map = (Map<String, Object>) d;
          startMap(map);
          writeMap(map);
          endMap(map);
        } else if (d instanceof Tuple) {
          Tuple t = (Tuple) d;
          writeTuple(t);
        } else if (d instanceof DataBag) {
          DataBag bag = (DataBag) d;
          writeBag(bag);
        } else if (d instanceof Boolean) {
          writeBool((Boolean) d);
        } else if (d instanceof Byte) {
          writeByte((Byte) d);
        } else if (d instanceof Integer) {
          writeInt((Integer) d);
        } else if (d instanceof Long) {
          writeLong((Long) d);
        } else if (d instanceof Float) {
          writeFloat((Float) d);
        } else if (d instanceof Double) {
          writeDouble((Double) d);
        } else if (d instanceof String) {
          writeString((String) d);
        } else if (d instanceof DataByteArray) {
          writeBuffer((DataByteArray) d);
        } else {
          throw new ExecException("Unknown data type");
        }
      } else { // if d is null, write nothing except ','
        writeNull();
      }
    }
  }
}