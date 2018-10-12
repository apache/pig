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

package org.apache.pig.impl.bloom;

public abstract class Hash {
    public static final int MURMUR = 1;
    public static final int MURMUR3 = MURMUR;
    public static final int MURMUR3KM = 2;
    public static final int JENKINS = 3;

    public static int parseHashType(String hashType) {
        if("murmur".equalsIgnoreCase(hashType)) {
            return MURMUR3;
        } else if("murmur3km".equalsIgnoreCase(hashType)) {
            return MURMUR3KM;
        } else if("jenkins".equalsIgnoreCase(hashType)) {
            return JENKINS;
        }
        throw new IllegalArgumentException("Hash Algorithm values must be one of - murmur, murmur3km, jenkins");
    }

    public static Hash getInstance(int hashType) {
        switch (hashType) {
            case MURMUR:
                return new Murmur3Hash();
            case MURMUR3KM:
                return new KirschMitzenmacherHash();
            case JENKINS:
                return new JenkinsHash();
        }
        throw new IllegalArgumentException("Hash type values must be one of - 1 (murmur), 2 (murmur3km), 3 (jenkins)");
    }

    /**
     * @param bytes
     * @param maxValue The maximum hashed value
     * @param nbHash The number of hashed values
     * @return
     */
    public abstract int[] hash(byte[] bytes, int maxValue, int nbHash);

}
