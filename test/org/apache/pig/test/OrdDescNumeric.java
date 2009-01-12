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
/*
package org.apache.pig.test;

import java.io.IOException;

import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.ComparisonFunc;

public class OrdDescNumeric extends ComparisonFunc {
    public int compare(Tuple t1, Tuple t2) {
        try {
            for (int i = 0; i < t1.arity(); i++) {
                Datum d1 = t1.getField(i);
                Datum d2 = t2.getField(i);
                int comp;
                if (d1 instanceof DataAtom) {
                    comp = compare((DataAtom)d1, (DataAtom)d2);
                } else {
                    comp = compare((Tuple)d1, (Tuple)d2);
                }
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        } catch (IOException e) {
            throw new RuntimeException("Error comparing keys in OrdDEscNumeric", e);
        }
    }
    
    private int compare(DataAtom a1, DataAtom a2) throws IOException {
        double num1 = a1.numval();
        double num2 = a2.numval();
        if (num2 > num1) {
            return 1;
        } else if (num2 < num1) {
            return -1;
        }
        return 0;
    }
}
*/
