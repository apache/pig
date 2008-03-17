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
package org.apache.pig.impl.eval.cond;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Datum;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.eval.EvalSpec;



public class CompCond extends Cond {
    private static final long serialVersionUID = 1L;
    
    public String op;   // one of "<", ">", "==", etc.
    public EvalSpec left, right;
    
    


    public CompCond(EvalSpec left, String op, EvalSpec right) throws IOException{
        this.op = op.toLowerCase();
        this.left = left;
        this.right = right;
        
        if (left.isAsynchronous() || right.isAsynchronous()){
            throw new IOException("Can't compare the output of an asynchronous function");
        }
    } 
   
    @Override
    public List<String> getFuncs() {
        return new ArrayList<String>();
    }

    @Override
    public boolean eval(Datum input) {
        
        Datum d1 = left.simpleEval(input);
        Datum d2 = right.simpleEval(input);
        
        if (!(d1 instanceof DataAtom) || !(d2 instanceof DataAtom)){
            throw new RuntimeException("Builtin functions cannot be used to compare non-atomic values. Use a filter function instead.");
        }
        
        DataAtom da1 = (DataAtom)d1;
        DataAtom da2 = (DataAtom)d2;
        
        
        char op1 = op.charAt(0);
        char op2 = op.length() >= 2 ? op.charAt(1) : '0';
        char op3 = op.length() == 3 ? op.charAt(2) : '0';
        
        switch (op1) {
            // numeric ops first
        case '=':
            if (op2 == '=') {
                return da1.numval().equals(da2.numval());
            } else {
                throw new RuntimeException("Internal error: Invalid filter operator: " + op);
            }
        case '<':
            if (op2 == '=') {
                return da1.numval().compareTo(da2.numval()) <= 0;
            } else {
                return da1.numval().compareTo(da2.numval()) < 0;
            }
        case '>':
            if (op2 == '=') {
                return da1.numval().compareTo(da2.numval()) >= 0;
            } else {
                return da1.numval().compareTo(da2.numval()) > 0;
            }
        case '!':
            if (op2 == '=') {
                return !da1.numval().equals(da2.numval());
            } else {
                throw new RuntimeException("Internal error: Invalid filter operator: " + op);
            }
            // now string ops
        case 'e':
            if (op2 == 'q') {
                return da1.equals(da2);
            } else {
                throw new RuntimeException("Internal error: Invalid filter operator: " + op);
            }
        case 'l':
            if (op2 == 't' && op3 == 'e') {
                return da1.compareTo(da2) <= 0;
            } else {
                return da1.compareTo(da2) < 0;
            }
        case 'g':
            if (op2 == 't' && op3 == 'e') {
                return da1.compareTo(da2) >= 0;
            } else {
                return da1.compareTo(da2) > 0;
            }
        case 'n':
            if (op2 == 'e' && op3 == 'q') {
                return !da1.equals(da2);
            } else {
                throw new RuntimeException("Internal error: Invalid filter operator: " + op);
            }
        default:
            throw new RuntimeException("Internal error: Invalid filter operator: " + op);
        }
    }
        
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(left);
        sb.append(" ");
        sb.append(op);
        sb.append(" ");
        sb.append(right);
        sb.append(")");
        return sb.toString();
    }
    
    @Override
    public void finish() {
        left.finish();
        right.finish();
    }


    @Override
    public void instantiateFunc(FunctionInstantiator instantiaor)
            throws IOException {
        left.instantiateFunc(instantiaor);
        right.instantiateFunc(instantiaor);        
    }
}
