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
package org.apache.pig.impl.logicalLayer;


import org.apache.pig.impl.logicalLayer.schema.TupleSchema;
import org.apache.pig.impl.physicalLayer.IntermedResult;



public class LORead extends LogicalOperator {
    private static final long serialVersionUID = 1L;



    protected IntermedResult readFrom = null;

    boolean readsFromSplit = false;

     @Override
     public String toString() {
        StringBuffer result = new StringBuffer(super.toString());
          result.append(" (readsFromSplit: ");
          result.append(readsFromSplit);
          result.append(')');
          return result.toString();
    }
    //Since intermed result may have multiple outputs, which output do I read?
        public int splitOutputToRead = 0;

    public LORead(IntermedResult readFromIn) {
        super();
        readFrom = readFromIn;
    }

    public LORead(IntermedResult readFromIn, int outputToRead) {
        super();
        readsFromSplit = true;
        this.splitOutputToRead = outputToRead;
        readFrom = readFromIn;
    }

    public boolean readsFromSplit() {
        return readsFromSplit;
    }

    @Override
    public String name() {
        return "Read";
    }
    @Override
    public String arguments() {
        return alias;
    }

    @Override
    public TupleSchema outputSchema() {
        if (schema == null) {
            if (readFrom.lp != null && readFrom.lp.root != null
                && readFrom.lp.root.outputSchema() != null) {
                schema = readFrom.lp.root.outputSchema().copy();
            } else {
                schema = new TupleSchema();
            }
        }

        schema.removeAllAliases();
        schema.setAlias(alias);

        return schema;
    }



    @Override
    public int getOutputType() {
        return readFrom.getOutputType();
    }

    public IntermedResult getReadFrom() {
        return readFrom;
    }
}
