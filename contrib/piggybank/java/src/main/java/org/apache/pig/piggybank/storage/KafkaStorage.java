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

import kafka.producer.ProducerConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by Maria Valero on 17/03/15.
 *
 * This class take an alias from PIG and send his content to Kafka in JSON format
 *
 * Example:
 *   data = LOAD 'data' AS (a1:int,a2:int,a3:int);
 *   STORE data INTO 'file-ignored' USING org.apache.pig.piggybank.storageKafkaStorage('topic','localhost:9092');
 */
public class KafkaStorage extends StoreFunc {
    private Producer<String, String> kafkaProducer;
    private String topic;
    String kafkaServer;
    ByteArrayOutputStream mOut;
    Logger log;

    /*
     *  This Class need a topic and the kafkaServer to connect with
     */
    public KafkaStorage(String topic,String kafkaServer)
    {
        this.topic=topic;
        this.kafkaServer=kafkaServer;
        log = RbLogger.getLogger(KafkaStorage.class.getName());
    }

    /*
     * Return the OutputFormat associated with StoreFuncInterface
     */
    @Override
    public org.apache.hadoop.mapreduce.OutputFormat getOutputFormat() throws IOException {
        // Nothing to store in hadoop
        return new NullOutputFormat();
    }

    /*
     * Communicate to the storer the location where the data needs to be stored
     */
    @Override
    public void setStoreLocation(String s, org.apache.hadoop.mapreduce.Job job) throws IOException {
        // IGNORE
    }

    /*
     * Initialize StoreFuncInterface to write data.
     */
    @Override
    public void prepareToWrite(org.apache.hadoop.mapreduce.RecordWriter recordWriter) throws IOException {
        // We set the kafka URL and topic
        final Properties props = new Properties();
        props.put("metadata.broker.list", kafkaServer);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        kafkaProducer = new Producer<String, String>(new ProducerConfig(props));
    }

    /*
     * Write a tuple to the data store
     */
    @Override
    public void putNext(Tuple tuple) throws IOException {
        if(tuple.get(0) == null) {
            return;
        }

        JSONObject jsonObj;
        jsonObj=new JSONObject();
        // We store a string from tuple
        for (int i=0 ; i < tuple.size() ; i++ ) {
            Object field;
            try {
                field = tuple.get(i);
            } catch (ExecException ee) {
                throw ee;
            }

            putField(field, i,jsonObj);
        }

        // We send to kafka
        kafkaProducer.send(new KeyedMessage<String, String>(topic, jsonObj.toString()));
    }

    // We set the field from Tuple in JSONObject
    @SuppressWarnings("unchecked")
    private void putField(Object field, int i,JSONObject jsonObj) throws IOException {

        try {
            switch (DataType.findType(field)) {
                case DataType.NULL:
                    jsonObj.put(String.valueOf(i), "NULL");
                    break;
                case DataType.BOOLEAN:
                    jsonObj.put(String.valueOf(i), (boolean) field);
                    break;
                case DataType.INTEGER:
                    jsonObj.put(String.valueOf(i), (int) field);
                    break;
                case DataType.LONG:
                    jsonObj.put(String.valueOf(i), (long) field);
                    break;
                case DataType.FLOAT:
                    jsonObj.put(String.valueOf(i), (float) field);
                    break;
                case DataType.DOUBLE:
                    jsonObj.put(String.valueOf(i), (double) field);
                    break;
                case DataType.BYTEARRAY:
                    byte[] b = ((DataByteArray) field).get();
                    jsonObj.put(String.valueOf(i), b);
                    break;
                case DataType.CHARARRAY:
                    jsonObj.put(String.valueOf(i), (String) field);
                    break;
                case DataType.BYTE:
                    jsonObj.put(String.valueOf(i), (byte) field);
                    break;

                case DataType.MAP:
                    boolean mapHasNext = false;
                    Map<String, Object> m = (Map<String, Object>)field;
                    for(Map.Entry<String, Object> e: m.entrySet()) {
                        jsonObj.put(e.getKey(), e.getValue());
                    }
                    break;

                case DataType.TUPLE:
                    Tuple t = (Tuple)field;
                    for(int n = 0; n < t.size(); ++n) {
                        try {
                            putField(t.get(n), i,jsonObj);
                        } catch (ExecException ee) {
                            throw ee;
                        }
                    }
                    break;

                case DataType.BAG:
                    Iterator<Tuple> tupleIter = ((DataBag)field).iterator();
                    while(tupleIter.hasNext()) {
                        putField(tupleIter.next(),i,jsonObj);
                    }
                    break;

                default:
                    throw new RuntimeException("Unknown datatype " + DataType.findType(field));
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}
