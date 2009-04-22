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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * LOProject is designed like a singly linked list; A few examples will
 * illustrate the point about the linked list nature of the design;
 * a = load 'input1' as (name, age);
 * b = group a by name;
 * foreach b generate a, a.name;
 * The project operator occurs in two places in the above script:
 * generate a(here) and a.name(here)
 * In the first occurrence, we are trying to project the elements of
 * the bag a; In order to retrieve the bag, we need to project the
 * the second column ($1) or column number 1 (using the zero based index)
 * from the input (the relation or bag b)
 * In the second occurence, we are trying to project the first column
 * ($0) or column number 0 from the bag a which in turn is the column
 * number 1 in the relation b; As you can see, the nested structure or
 * the singly linked list nature is clearly visible;
 * Given that it's a singly linked list, the null pointer or the sentinel
 * is marked explictly using the boolean variable mSentinel; The sentinel
 * is marked true only when the input is a relational operator; This occurs
 * when we create the innermost operator
 */
public class LOProject extends ExpressionOperator {
    private static final long serialVersionUID = 2L;

    /**
     * The expression and the column to be projected.
     */
    private LogicalOperator mExp;
    private List<Integer> mProjection;
    private boolean mIsStar = false;
    private static Log log = LogFactory.getLog(LOProject.class);
    private boolean mSentinel;
    private boolean mOverloaded = false;

    private boolean sendEmptyBagOnEOP = false;

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param key
     *            Operator key to assign to this node.
     * @param exp
     *            the expression which might contain the column to project
     * @param projection
     *            the list of columns to project
     */
    public LOProject(LogicalPlan plan, OperatorKey key, LogicalOperator exp,
            List<Integer> projection) {
        super(plan, key);
        mExp = exp;
        mProjection = projection;
        if(mExp instanceof ExpressionOperator) {
            mSentinel = false;
        } else {
            mSentinel = true;
        }
    }

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param key
     *            Operator key to assign to this node.
     * @param exp
     *            the expression which might contain the column to project
     * @param projection
     *            the column to project
     */
    public LOProject(LogicalPlan plan, OperatorKey key, LogicalOperator exp,
            Integer projection) {
        super(plan, key);
        mExp = exp;
        mProjection = new ArrayList<Integer>(1);
        mProjection.add(projection);
        if(mExp instanceof ExpressionOperator) {
            mSentinel = false;
        } else {
            mSentinel = true;
        }
    }

    public LogicalOperator getExpression() {
        return mExp;
    }

    public void setExpression(LogicalOperator exp) {
        mExp = exp;
    }

    public boolean isStar() { 
        return mIsStar;
    }

    public List<Integer> getProjection() {
        return mProjection;
    }

    public void setProjection(List<Integer> proj) {
        mProjection = proj;
    }

    public int getCol() {
        if (mProjection.size() != 1)
            
            throw new RuntimeException(
                    "Internal error: improper use of getCol in "
                            + LOProject.class.getName());
        return mProjection.get(0);

    }

    public void setStar(boolean b) {
        mIsStar = b;
    }

    public boolean getSentinel() {
        return mSentinel;
    }

    public void setSentinel(boolean b) {
        mSentinel = b;
    }

    public boolean getOverloaded() {
        return mOverloaded;
    }

    public void setOverloaded(boolean b) {
        mOverloaded = b;
    }

    @Override
    public String name() {
        return "Project " + mKey.scope + "-" + mKey.id + " Projections: " + (mIsStar? " [*] ": mProjection) + " Overloaded: " + mOverloaded;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public Schema.FieldSchema getFieldSchema() throws FrontendException {
        log.debug("Inside getFieldSchemas");
        log.debug("Number of columns: " + mProjection.size());
        for (int i : mProjection) {
            log.debug("Column: " + i);
        }
        LogicalOperator expressionOperator = mExp;
        log.debug("expressionOperator = " + expressionOperator);
        log.debug("mIsStar: " + mIsStar);

        if (!mIsFieldSchemaComputed) {

            if (mIsStar) {
                log.debug("mIsStar is true");
                try {
                    if (null != expressionOperator) {
                        log.debug("expressionOperator is not null "
                                + expressionOperator.getClass().getName() + " " + expressionOperator);
                        if(!mSentinel) {
                            //we have an expression operator and hence a list of field shcemas
                            Schema.FieldSchema fs = ((ExpressionOperator)expressionOperator).getFieldSchema();
                            mFieldSchema = new Schema.FieldSchema(fs);
                            mFieldSchema.setParent(fs.canonicalName, expressionOperator);
                        } else {
                            //we have a relational operator as input and hence a schema
                            log.debug("expression operator alias: " + expressionOperator.getAlias());
                            log.debug("expression operator schema: " + expressionOperator.getSchema());
                            log.debug("expression operator type: " + expressionOperator.getType());
                            //TODO
                            //the type of the operator will be unknown. when type checking is in place
                            //add the type of the operator as a parameter to the fieldschema creation
                            mFieldSchema = new Schema.FieldSchema(expressionOperator.getAlias(), expressionOperator.getSchema(), DataType.TUPLE);
                            mFieldSchema.setParent(null, expressionOperator);
                            //mFieldSchema = new Schema.FieldSchema(expressionOperator.getAlias(), expressionOperator.getSchema());
                        }
                    } else {
                        log.warn("The input for a projection operator cannot be null");
                    }
                    mIsFieldSchemaComputed = true;
                } catch (FrontendException fee) {
                    mFieldSchema = null;
                    mIsFieldSchemaComputed = false;
                    throw fee;
                }
            } else {
                //its n list of columns to project including a single column
                List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>(mProjection.size());
                //try {
                    if (null != expressionOperator) {
                        log.debug("expressionOperator is not null");
                        if(mProjection.size() == 1) {
                            //if there is only one element then extract and return the field schema
                            log.debug("Only one element");
                            if(!mSentinel) {
                                log.debug("Input is an expression operator");
                                Schema.FieldSchema expOpFs = ((ExpressionOperator)expressionOperator).getFieldSchema();
                                if(null != expOpFs) {
                                    Schema s = expOpFs.schema;
                                    if(null != s) {
                                        Schema.FieldSchema fs;
                                        if(s.isTwoLevelAccessRequired()) {
                                            // this is the case where the schema is that of
                                            // a bag which has just one tuple fieldschema which
                                            // in turn has a list of fieldschemas. So the field
                                            // schema we are trying to construct would be of the
                                            // item we are trying to project inside the tuple 
                                            // fieldschema - because currently when we say b.i where
                                            // b is a bag, we are trying to access the item i
                                            // present in the tuple in the bag.
                                            
                                            // check that indeed we only have one field schema
                                            // which is that of a tuple
                                            if(s.getFields().size() != 1) {
                                                int errCode = 1008;
                                                String msg = "Expected a bag schema with a single " +
                                                "element of type "+ DataType.findTypeName(DataType.TUPLE) +
                                                " but got a bag schema with multiple elements.";
                                                throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
                                            }
                                            Schema.FieldSchema tupleFS = s.getField(0);
                                            if(tupleFS.type != DataType.TUPLE) {
                                                int errCode = 1009;
                                                String msg = "Expected a bag schema with a single " +
                                                "element of type "+ DataType.findTypeName(DataType.TUPLE) +
                                                " but got an element of type " +
                                                DataType.findTypeName(tupleFS.type);
                                                throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
                                            }
                                            fs = tupleFS.schema.getField(mProjection.get(0));
                                        } else {
                                            // normal single level access
                                            fs = s.getField(mProjection.get(0));
                                        }
                                        mFieldSchema = new Schema.FieldSchema(fs);
                                        mFieldSchema.setParent(fs.canonicalName, expressionOperator);
                                    } else {
                                        mFieldSchema = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                                        mFieldSchema.setParent(expOpFs.canonicalName, expressionOperator);
                                    }
                                } else {
                                    mFieldSchema = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                                    mFieldSchema.setParent(null, expressionOperator);
                                }
                            } else {
                                log.debug("Input is a logical operator");
                                Schema s = expressionOperator.getSchema();
                                log.debug("s: " + s);
                                if(null != s) {
                                    Schema.FieldSchema fs = s.getField(mProjection.get(0));
                                    mFieldSchema = new Schema.FieldSchema(fs);
                                    mFieldSchema.setParent(fs.canonicalName, expressionOperator);
                                    log.debug("mFieldSchema alias: " + mFieldSchema.alias);
                                    log.debug("mFieldSchema schema: " + mFieldSchema.schema);
                                } else {
                                    mFieldSchema = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                                    mFieldSchema.setParent(null, expressionOperator);
                                }
                                mType = mFieldSchema.type ;
                            }
                            mIsFieldSchemaComputed = true;
                            return mFieldSchema;
                        }
                        
                        for (int colNum : mProjection) {
                            log.debug("Col: " + colNum);
                            Schema.FieldSchema fs;
                            if(!mSentinel) {
                                Schema.FieldSchema expOpFs = ((ExpressionOperator)expressionOperator).getFieldSchema();
                                if(null != expOpFs) {
                                    Schema s = expOpFs.schema;
                                    log.debug("Schema s: " + s);
                                    if(null != s) {
                                        if(colNum < s.size()) {
                                            Schema.FieldSchema parentFs = s.getField(colNum);
                                            fs = new Schema.FieldSchema(parentFs);
                                            fss.add(fs);
                                            fs.setParent(parentFs.canonicalName, expressionOperator);
                                        } else {
                                            fs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                                            fss.add(fs);
                                            fs.setParent(expOpFs.canonicalName, expressionOperator);
                                        }
                                    } else {
                                        fs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                                        fss.add(fs);
                                        fs.setParent(expOpFs.canonicalName, expressionOperator);
                                    }
                                } else {
                                    fs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                                    fss.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
                                    fs.setParent(null, expressionOperator);
                                }
                            } else {
                                Schema s = expressionOperator.getSchema();
                                if(null != s) {
                                    Schema.FieldSchema parentFs = s.getField(colNum);
                                    fs = new Schema.FieldSchema(parentFs);
                                    fss.add(fs);
                                    fs.setParent(parentFs.canonicalName, expressionOperator);
                                } else {
                                    fs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                                    fss.add(fs);
                                    fs.setParent(null, expressionOperator);
                                }
                            }
                        }
    
                    } else {
                        log.warn("The input for a projection operator cannot be null");
                        //fss.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
                    }
                //} catch(ParseException pe) {
                //    mFieldSchema = null;
                //    mIsFieldSchemaComputed = false;
                //    throw new FrontendException(pe.getMessage());
                //}
                mFieldSchema = new Schema.FieldSchema(expressionOperator.getAlias(), new Schema(fss));
                mFieldSchema.setParent(null, expressionOperator);
                mIsFieldSchemaComputed = true;
            }

        }

        if(null != mFieldSchema) {
            mType = mFieldSchema.type;
        }
        
        List<LogicalOperator> succList = mPlan.getSuccessors(this) ;
        List<LogicalOperator> predList = mPlan.getPredecessors(this) ;
        if((null != succList) && !(succList.get(0) instanceof ExpressionOperator)) {
            if(!DataType.isSchemaType(mType)) {
                Schema pjSchema = new Schema(mFieldSchema);
                mFieldSchema = new Schema.FieldSchema(getAlias(), pjSchema, DataType.TUPLE);
                mFieldSchema.setParent(null, expressionOperator);
            } else {
                if(null != mFieldSchema) {
                    mFieldSchema.type = DataType.TUPLE;
                }
            }
            setOverloaded(true);
            setType(DataType.TUPLE);
        } else if(null != predList) {
            LogicalOperator projectInput = getExpression();
            if(((projectInput instanceof LOProject) || !(predList.get(0) instanceof ExpressionOperator)) && (projectInput.getType() == DataType.BAG)) {
                if(!DataType.isSchemaType(mType)) {
                    Schema pjSchema = new Schema(mFieldSchema);
                    mFieldSchema = new Schema.FieldSchema(getAlias(), pjSchema, DataType.BAG);
                    mFieldSchema.setParent(null, expressionOperator);
                } else {
                    if(null != mFieldSchema) {
                        mFieldSchema.type = DataType.BAG;
                    }
                }
                setType(DataType.BAG);
            }
        }
        
        log.debug("Exiting getFieldSchema");
        return mFieldSchema;
    }

    public boolean isSingleProjection()  {
        return mProjection.size() == 1 ;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public Schema getSchema() throws FrontendException{
        // Called to make sure we've constructed the field schema before trying
        // to read it.
        getFieldSchema();
        if (mFieldSchema != null){
            return mFieldSchema.schema ;
        }
        else {
            return null ;
        }
    }

    /* For debugging only */
    public String toDetailString() {
        StringBuilder sb = new StringBuilder() ;
        sb.append("LOProject") ;
        sb.append(" Id=" + this.mKey.id) ;
        sb.append(" Projection=") ;
        boolean isFirst = true ;
        for(int i=0;i< mProjection.size();i++) {
            if (isFirst) {
                isFirst = false ;
            }
            else {
                sb.append(",") ;
            }
            sb.append(mProjection.get(i)) ;
        }
        sb.append(" isStart=") ;
        sb.append(mIsStar) ;
        sb.append(" isSentinel=") ;
        sb.append(mSentinel) ;
        return sb.toString() ;
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.ExpressionOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        LOProject clone = (LOProject)super.clone();
        
        // deep copy project specific attributes
        clone.mProjection = new ArrayList<Integer>();
        for (Iterator<Integer> it = mProjection.iterator(); it.hasNext();) {
            clone.mProjection.add(new Integer(it.next()));
        }
        
        return clone;
    }

    /**
     * @param sendEmptyBagOnEOP the sendEmptyBagOnEOP to set
     */
    public void setSendEmptyBagOnEOP(boolean sendEmptyBagOnEOP) {
        this.sendEmptyBagOnEOP = sendEmptyBagOnEOP;
    }

    /**
     * @return the sendEmptyBagOnEOP
     */
    public boolean isSendEmptyBagOnEOP() {
        return sendEmptyBagOnEOP;
    }

}
