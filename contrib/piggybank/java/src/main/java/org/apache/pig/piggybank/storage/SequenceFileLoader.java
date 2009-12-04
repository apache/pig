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
package org.apache.pig.piggybank.storage;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.impl.logicalLayer.FrontendException;

import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.SamplableLoader;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A Loader for Hadoop-Standard SequenceFiles.
 * able to work with the following types as keys or values:
 * Text, IntWritable, LongWritable, FloatWritable, DoubleWritable, BooleanWritable, ByteWritable
 **/

public class SequenceFileLoader implements LoadFunc, SamplableLoader {
  
  private SequenceFile.Reader reader;
  private long end;
  private Writable key;
  private Writable value;
  private ArrayList<Object> mProtoTuple = null;
  
  protected static final Log LOG = LogFactory.getLog(SequenceFileLoader.class);
  protected TupleFactory mTupleFactory = TupleFactory.getInstance();
  protected SerializationFactory serializationFactory;

  protected byte keyType;
  protected byte valType;
    
  public SequenceFileLoader() {
  
  }
  
  @Override
  public void bindTo(String fileName, BufferedPositionedInputStream is,
      long offset, long end) throws IOException {
    
    inferReader(fileName);
    if (offset != 0)
      reader.sync(offset);

    this.end = end;
    
    try {
      this.key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), PigMapReduce.sJobConf);
      this.value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), PigMapReduce.sJobConf);
    } catch (ClassCastException e) {
      throw new RuntimeException("SequenceFile contains non-Writable objects", e);
    }
    setKeyValueTypes(key.getClass(), value.getClass());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Schema determineSchema(String fileName, ExecType execType,
      DataStorage storage) throws IOException {
    inferReader(fileName);
    Class<Writable> keyClass = null;
    Class<Writable> valClass= null;
    try {
      keyClass = (Class<Writable>) reader.getKeyClass();
      valClass = (Class<Writable>) reader.getValueClass();
    } catch (ClassCastException e) {
      throw new RuntimeException("SequenceFile contains non-Writable objects", e);
    }
    Schema schema = new Schema();
    setKeyValueTypes(keyClass, valClass);  
    schema.add(new Schema.FieldSchema(null, keyType));
    schema.add(new Schema.FieldSchema(null, valType));
    return schema;
  }

  protected void setKeyValueTypes(Class<?> keyClass, Class<?> valueClass) throws BackendException {
    this.keyType |= inferPigDataType(keyClass);
    this.valType |= inferPigDataType(valueClass);
    if (keyType == DataType.ERROR) { 
      LOG.warn("Unable to translate key "+key.getClass()+" to a Pig datatype");
      throw new BackendException("Unable to translate "+key.getClass()+" to a Pig datatype");
    } 
    if (valType == DataType.ERROR) {
      LOG.warn("Unable to translate value "+value.getClass()+" to a Pig datatype");
      throw new BackendException("Unable to translate "+value.getClass()+" to a Pig datatype");
    }

  }
  protected void inferReader(String fileName) throws IOException {
    if (reader == null) {
      Configuration conf = new Configuration();
      Path path = new Path(fileName);
      FileSystem fs = FileSystem.get(path.toUri(), conf);
      reader = new SequenceFile.Reader(fs, path, conf);
    }
  }
  
  protected byte inferPigDataType(Type t) {
    if (t == DataByteArray.class) return DataType.BYTEARRAY;
    else if (t == Text.class) return DataType.CHARARRAY;
    else if (t == IntWritable.class) return DataType.INTEGER;
    else if (t == LongWritable.class) return DataType.LONG;
    else if (t == FloatWritable.class) return DataType.FLOAT;
    else if (t == DoubleWritable.class) return DataType.DOUBLE;
    else if (t == BooleanWritable.class) return DataType.BOOLEAN;
    else if (t == ByteWritable.class) return DataType.BYTE;
    // not doing maps or other complex types for now
    else return DataType.ERROR;
  }
  
  protected Object translateWritableToPigDataType(Writable w, byte dataType) {
    switch(dataType) {
      case DataType.CHARARRAY: return ((Text) w).toString();
      case DataType.BYTEARRAY: return((DataByteArray) w).get();
      case DataType.INTEGER: return ((IntWritable) w).get();
      case DataType.LONG: return ((LongWritable) w).get();
      case DataType.FLOAT: return ((FloatWritable) w).get();
      case DataType.DOUBLE: return ((DoubleWritable) w).get();
      case DataType.BYTE: return ((ByteWritable) w).get();
    }
    
    return null;
  }
  
  @Override
  public LoadFunc.RequiredFieldResponse fieldsToRead(LoadFunc.RequiredFieldList requiredFieldList) throws FrontendException {
      return new LoadFunc.RequiredFieldResponse(false);
  }

  @Override
  public Tuple getNext() throws IOException {
    if (mProtoTuple == null) mProtoTuple = new ArrayList<Object>(2);
    if (reader != null && (reader.getPosition() < end || !reader.syncSeen()) && reader.next(key, value)) {
      mProtoTuple.add(translateWritableToPigDataType(key, keyType));
      mProtoTuple.add(translateWritableToPigDataType(value, valType));
      Tuple t =  mTupleFactory.newTuple(mProtoTuple);
      mProtoTuple.clear();
      return t;
    }
    return null;
  }

  @Override
  public long getPosition() throws IOException {
    return reader.getPosition();
  }

  @Override
  public Tuple getSampledTuple() throws IOException {
    return this.getNext();
  }

  @Override
  public long skip(long n) throws IOException {
    long startPos = reader.getPosition();
    reader.sync(startPos+n);
    return reader.getPosition()-startPos;
  }

  @Override
  public DataBag bytesToBag(byte[] b) throws IOException {
    throw new FrontendException("SequenceFileLoader does not expect to cast data.");
  }

  @Override
  public String bytesToCharArray(byte[] b) throws IOException {
    throw new FrontendException("SequenceFileLoader does not expect to cast data.");
  }

  @Override
  public Double bytesToDouble(byte[] b) throws IOException {
    throw new FrontendException("SequenceFileLoader does not expect to cast data.");
  }

  @Override
  public Float bytesToFloat(byte[] b) throws IOException {
    throw new FrontendException("SequenceFileLoader does not expect to cast data.");
  }

  @Override
  public Integer bytesToInteger(byte[] b) throws IOException {
    throw new FrontendException("SequenceFileLoader does not expect to cast data.");
  }

  @Override
  public Long bytesToLong(byte[] b) throws IOException {
    throw new FrontendException("SequenceFileLoader does not expect to cast data.");
  }

  @Override
  public Map<String, Object> bytesToMap(byte[] b) throws IOException {
    throw new FrontendException("SequenceFileLoader does not expect to cast data.");
  }

  @Override
  public Tuple bytesToTuple(byte[] b) throws IOException {
    throw new FrontendException("SequenceFileLoader does not expect to cast data.");
  }
}
