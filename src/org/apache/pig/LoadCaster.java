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
package org.apache.pig;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 *
 */
public interface LoadCaster {

    /**
     * Cast data from bytes to long value.  
     * @param b byte array to be cast.
     * @return Long value.
     * @throws IOException if the value cannot be cast.
     */
    public Long bytesToLong(byte[] b) throws IOException;

    /**
     * Cast data from bytes to float value.  
     * @param b byte array to be cast.
     * @return Float value.
     * @throws IOException if the value cannot be cast.
     */
    public Float bytesToFloat(byte[] b) throws IOException;

    /**
     * Cast data from bytes to double value.  
     * @param b byte array to be cast.
     * @return Double value.
     * @throws IOException if the value cannot be cast.
     */
    public Double bytesToDouble(byte[] b) throws IOException;

    /**
     * Cast data from bytes to integer value.  
     * @param b byte array to be cast.
     * @return Double value.
     * @throws IOException if the value cannot be cast.
     */
    public Integer bytesToInteger(byte[] b) throws IOException;
    
    /**
     * Cast data from bytes to chararray value.  
     * @param b byte array to be cast.
     * @return String value.
     * @throws IOException if the value cannot be cast.
     */
    public String bytesToCharArray(byte[] b) throws IOException;

    /**
     * Cast data from bytes to map value.  
     * @param b byte array to be cast.
     * @return Map value.
     * @throws IOException if the value cannot be cast.
     */
    public Map<String, Object> bytesToMap(byte[] b) throws IOException;

    /**
     * Cast data from bytes to tuple value.  
     * @param b byte array to be cast.
     * @param fieldSchema field schema for the output tuple
     * @return Tuple value.
     * @throws IOException if the value cannot be cast.
     */
    public Tuple bytesToTuple(byte[] b, ResourceFieldSchema fieldSchema) throws IOException;

    /**
     * Cast data from bytes to bag value.  
     * @param b byte array to be cast.
     * @param fieldSchema field schema for the output bag
     * @return Bag value.
     * @throws IOException if the value cannot be cast.
     */
    public DataBag bytesToBag(byte[] b, ResourceFieldSchema fieldSchema) throws IOException;

}
