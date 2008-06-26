package org.apache.pig.impl.physicalLayer.relationalOperators;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class POForEach extends PhysicalOperator {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private List<Boolean> isToBeFlattened;
    private List<PhysicalPlan> inputPlans;
    private List<PhysicalOperator> planLeaves = new LinkedList<PhysicalOperator>();
    private Log log = LogFactory.getLog(getClass());
    //Since the plan has a generate, this needs to be maintained
    //as the generate can potentially return multiple tuples for
    //same call.
    private boolean processingPlan = false;
    
    //its holds the iterators of the databags given by the input expressions which need flattening.
    Iterator<Tuple> [] its = null;
    
    //This holds the outputs given out by the input expressions of any datatype
    Object [] bags = null;
    
    //This is the template whcih contains tuples and is flattened out in CreateTuple() to generate the final output
    Object[] data = null;
    
    public POForEach(OperatorKey k) {
        this(k,-1,null,null);
    }

    public POForEach(OperatorKey k, int rp, List inp) {
        this(k,rp,inp,null);
    }

    public POForEach(OperatorKey k, int rp) {
        this(k,rp,null,null);
    }

    public POForEach(OperatorKey k, List inp) {
        this(k,-1,inp,null);
    }
    
    public POForEach(OperatorKey k, int rp, List<PhysicalPlan> inp, List<Boolean>  isToBeFlattened){
        super(k, rp);
        this.isToBeFlattened = isToBeFlattened;
        this.inputPlans = inp;
        getLeaves();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitPOForEach(this);
    }

    @Override
    public String name() {
        String fString = getFlatStr();
        return "New For Each" + "(" + fString + ")" + "[" + DataType.findTypeName(resultType) + "]" +" - " + mKey.toString();
    }
    
    private String getFlatStr() {
        if(isToBeFlattened==null)
            return "";
        StringBuilder sb = new StringBuilder();
        for (Boolean b : isToBeFlattened) {
            sb.append(b);
            sb.append(',');
        }
        if(sb.length()>0){
            sb.deleteCharAt(sb.length()-1);
        }
        return sb.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }
    
    /**
     * Calls getNext on the generate operator inside the nested
     * physical plan and returns it maintaining an additional state
     * to denote the begin and end of the nested plan processing.
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        Result res = null;
        Result inp = null;
        //The nested plan is under processing
        //So return tuples that the generate oper
        //returns
        if(processingPlan){
            while(true) {
                res = processPlan();
                if(res.returnStatus==POStatus.STATUS_OK){
                    return res;
                }
                if(res.returnStatus==POStatus.STATUS_EOP){
                    processingPlan = false;
                    break;
                }
                if(res.returnStatus==POStatus.STATUS_ERR)
                    return res;
                if(res.returnStatus==POStatus.STATUS_NULL)
                    continue;
            }
        }
        //The nested plan processing is done or is
        //yet to begin. So process the input and start
        //nested plan processing on the input tuple
        //read
        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR)
                return inp;
            if (inp.returnStatus == POStatus.STATUS_NULL)
                continue;
            
            attachInputToPlans((Tuple) inp.result);
            
            res = processPlan();
            
            processingPlan = true;
            
            return res;
        }
    }

    private Result processPlan() throws ExecException{
        int noItems = planLeaves.size();
        Result res = new Result();
        
        //We check if all the databags have exhausted the tuples. If so we enforce the reading of new data by setting data and its to null
        if(its != null) {
            boolean restartIts = true;
            for(int i = 0; i < noItems; ++i) {
                if(its[i] != null && isToBeFlattened.get(i) == true)
                    restartIts &= !its[i].hasNext();
            }
            //this means that all the databags have reached their last elements. so we need to force reading of fresh databags
            if(restartIts) {
                its = null;
                data = null;
            }
        }
        
        if(its == null) {
            //getNext being called for the first time OR starting with a set of new data from inputs 
            its = new Iterator[noItems];
            bags = new Object[noItems];
            
            for(int i = 0; i < noItems; ++i) {
                //Getting the iterators
                //populate the input data
                Result inputData = null;
                Byte resultType = ((PhysicalOperator)planLeaves.get(i)).getResultType();
                switch(resultType) {
                case DataType.BAG : DataBag b = null;
                inputData = ((PhysicalOperator)planLeaves.get(i)).getNext(b);
                break;
                case DataType.TUPLE : Tuple t = null;
                inputData = ((PhysicalOperator)planLeaves.get(i)).getNext(t);
                break;
                case DataType.BYTEARRAY : DataByteArray db = null;
                inputData = ((PhysicalOperator)planLeaves.get(i)).getNext(db);
                break; 
                case DataType.MAP : Map map = null;
                inputData = ((PhysicalOperator)planLeaves.get(i)).getNext(map);
                break;
                case DataType.BOOLEAN : Boolean bool = null;
                inputData = ((PhysicalOperator)planLeaves.get(i)).getNext(bool);
                break;
                case DataType.INTEGER : Integer integer = null;
                inputData = ((PhysicalOperator)planLeaves.get(i)).getNext(integer);
                break;
                case DataType.DOUBLE : Double d = null;
                inputData = ((PhysicalOperator)planLeaves.get(i)).getNext(d);
                break;
                case DataType.LONG : Long l = null;
                inputData = ((PhysicalOperator)planLeaves.get(i)).getNext(l);
                break;
                case DataType.FLOAT : Float f = null;
                inputData = ((PhysicalOperator)planLeaves.get(i)).getNext(f);
                break;
                case DataType.CHARARRAY : String str = null;
                inputData = ((PhysicalOperator)planLeaves.get(i)).getNext(str);
                break;
                }
                
                if(inputData.returnStatus == POStatus.STATUS_EOP) {
                    //we are done with all the elements. Time to return.
                    its = null;
                    bags = null;
                    return inputData;
                }

//                Object input = null;
                
                bags[i] = inputData.result;
                
                if(inputData.result instanceof DataBag && isToBeFlattened.get(i)) 
                    its[i] = ((DataBag)bags[i]).iterator();
                else 
                    its[i] = null;
            }
        }
        
        Boolean done = false;
        while(!done) {
            if(data == null) {
                //getNext being called for the first time or starting on new input data
                //we instantiate the template array and start populating it with data
                data = new Object[noItems];
                for(int i = 0; i < noItems; ++i) {
                    if(isToBeFlattened.get(i) && bags[i] instanceof DataBag) {
                        if(its[i].hasNext()) {
                            data[i] = its[i].next();
                        } else {
                            //the input set is null, so we return
                            res.returnStatus = POStatus.STATUS_NULL;
                            return res;
                        }
                    } else {
                        data[i] = bags[i];
                    }
                    
                }
                if(reporter!=null) reporter.progress();
                //CreateTuple(data);
                res.result = CreateTuple(data);
                res.returnStatus = POStatus.STATUS_OK;
                return res;
            } else {
                //we try to find the last expression which needs flattening and start iterating over it
                //we also try to update the template array
                for(int index = noItems - 1; index >= 0; --index) {
                    if(its[index] != null && isToBeFlattened.get(index)) {
                        if(its[index].hasNext()) {
                            data[index] =  its[index].next();
                            res.result = CreateTuple(data);
                            res.returnStatus = POStatus.STATUS_OK;
                            return res;
                        }
                        else{
                            its[index] = ((DataBag)bags[index]).iterator();
                            data[index] = its[index].next();
                        }
                    }
                }
            }
        }
        
        return null;
    }
    
    /**
     * 
     * @param data array that is the template for the final flattened tuple
     * @return the final flattened tuple
     */
    private Tuple CreateTuple(Object[] data) throws ExecException {
        TupleFactory tf = TupleFactory.getInstance();
        Tuple out = tf.newTuple();
        for(int i = 0; i < data.length; ++i) {
            Object in = data[i];
            
            if(in instanceof Tuple) {
                Tuple t = (Tuple)in;
                for(int j = 0; j < t.size(); ++j) {
                    out.append(t.get(j));
                }
            } else
                out.append(in);
        }
        return out;
    }

    
    private void attachInputToPlans(Tuple t) {
        //super.attachInput(t);
        for(PhysicalPlan p : inputPlans) {
            p.attachInput(t);
        }
    }
    
    private void getLeaves() {
        if (inputPlans != null) {
            for(PhysicalPlan p : inputPlans) {
                planLeaves.add((PhysicalOperator)p.getLeaves().get(0));
            }
        }
    }
    
    public List<PhysicalPlan> getInputPlans() {
        return inputPlans;
    }

    public void setInputPlans(List<PhysicalPlan> plans) {
        inputPlans = plans;
        planLeaves.clear();
        getLeaves();
    }

    public void setToBeFlattened(List<Boolean> flattens) {
        isToBeFlattened = flattens;
    }


}
