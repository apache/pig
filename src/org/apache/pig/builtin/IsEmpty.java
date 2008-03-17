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
package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataMap;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;


public class IsEmpty extends FilterFunc {

    @Override
    public boolean exec(Tuple input) throws IOException {
        Datum values = input.getField(0);
        if (values instanceof DataBag) {
            return ((DataBag)values).size() == 0;
        } else if (values instanceof DataMap) {
            return ((DataMap)values).cardinality() == 0;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("Cannot test a ");
            sb.append(values.getClass().getSimpleName());
            sb.append(" for emptiness.");
            throw new IOException(sb.toString());
        }
    }

}
