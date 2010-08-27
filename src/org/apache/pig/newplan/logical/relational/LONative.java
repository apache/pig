package org.apache.pig.newplan.logical.relational;

import java.util.Arrays;

import org.apache.pig.impl.logicalLayer.FrontendException;

import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;

public class LONative extends LogicalRelationalOperator {

    private String nativeMRJar;
    private String[] params = null;
//    private LOLoad load;
//    private LOStore store;
    
    public LONative(OperatorPlan plan, String nativeJar, String[] parameters) {
        super("LONative", plan);
//        this.store = loStore;
//        this.load = loLoad;
        this.nativeMRJar = nativeJar;
        this.params = parameters;
        
    }

    @Override
    public LogicalSchema getSchema() throws FrontendException {
//        return load.getSchema();
        return null;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }

    @Override
    public boolean isEqual(Operator obj) throws FrontendException {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LONative other = (LONative) obj;

//        if (load == null) {
//            if (other.load != null)
//                return false;
//        } else if (!load.equals(other.load))
//            return false;
        if (nativeMRJar == null) {
            if (other.nativeMRJar != null)
                return false;
        } else if (!nativeMRJar.equals(other.nativeMRJar))
            return false;
        if (!Arrays.equals(params, other.params))
            return false;
//        if (store == null) {
//            if (other.store != null)
//                return false;
//        } else if (!store.equals(other.store))
//            return false;
//        
        //check predecessors and schema
        if(! checkEquality(other))
            return false;
        
        return true;
    }


    /**
     * @return the nativeMRJar
     */
    public String getNativeMRJar() {
        return nativeMRJar;
    }

    /**
     * @param nativeMRJar the nativeMRJar to set
     */
    public void setNativeMRJar(String nativeMRJar) {
        this.nativeMRJar = nativeMRJar;
    }

    /**
     * @return the params
     */
    public String[] getParams() {
        return params;
    }

    /**
     * @param params the params to set
     */
    public void setParams(String[] params) {
        this.params = params;
    }

//    /**
//     * @return the load
//     */
//    public LOLoad getLoad() {
//        return load;
//    }
//
//    /**
//     * @param load the load to set
//     */
//    public void setLoad(LOLoad load) {
//        this.load = load;
//    }
//
//    /**
//     * @return the store
//     */
//    public LOStore getStore() {
//        return store;
//    }
//
//    /**
//     * @param store the store to set
//     */
//    public void setStore(LOStore store) {
//        this.store = store;
//    }
//

}
