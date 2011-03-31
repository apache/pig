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

package org.apache.pig.test.udf.evalfunc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Test that constructor arguments are passed to sub functions in both the
 * algebraic and argToFuncMapping cases.  This function returns 
 * the constructor argument no matter what you pass it.
 */
public class TestConstructorArgs extends EvalFunc<String> implements Algebraic
{

    private String arg;

    public TestConstructorArgs() {
    }

    public TestConstructorArgs(String arg) {
        this.arg = arg;
    }

    //@Override
    public String exec(Tuple input) throws IOException 
    {
        return arg;
    }

    public String getInitial() {
        return Initial.class.getName();
    }

    public String getIntermed() {
        return Intermediate.class.getName();
    }

    public String getFinal() {
        return Final.class.getName();
    }


    static public class Initial extends EvalFunc<Tuple> {

        private String initialArg;

        public Initial() {
        }

        public Initial(String arg) {
            initialArg = arg;
        }

        public Tuple exec(Tuple input) throws IOException {
            if (initialArg == null) {
                throw new IOException("Expected initial arg to be non null");
            }
            return TupleFactory.getInstance().newTuple();
        }
    }

    static public class Intermediate extends EvalFunc<Tuple> {

        private String intermediateArg;

        public Intermediate(String arg) {
            intermediateArg = arg;
        }

        public Tuple exec(Tuple input) throws IOException {
            if (intermediateArg == null) {
                throw new IOException("Expected intermediate arg to be non null");
            }
            return TupleFactory.getInstance().newTuple();
        }
    }

    static public class Final extends EvalFunc<String> {

        private String finalArg;

        public Final(String arg) {
            finalArg = arg;
        }

        public String exec(Tuple input) throws IOException {
            return finalArg;
        }
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(),
            new Schema(new FieldSchema(null, DataType.CHARARRAY))));
        funcList.add(new FuncSpec(IntTest.class.getName(),
            new Schema(new FieldSchema(null, DataType.INTEGER))));
        return funcList;
    }

    static public class IntTest extends EvalFunc<String> {

        private String intTestArg;

        public IntTest(String arg) {
            intTestArg = arg;
        }

        public String exec(Tuple input) throws IOException {
            return intTestArg;
        }
    }
}
