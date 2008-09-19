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
package org.apache.pig.impl.io;

import org.apache.hadoop.io.Text;

/**
 *
 */
public class NullableText extends PigNullableWritable {

    public NullableText() {
        mValue = new Text();
    }

    /**
     * @param utf8
     */
    public NullableText(byte[] utf8) {
        mValue = new Text(utf8);
    }

    /**
     * @param string
     */
    public NullableText(String string) {
        mValue = new Text(string);
    }

    public Object getValueAsPigType() {
        return isNull() ? null : ((Text)mValue).toString();
    }
}
