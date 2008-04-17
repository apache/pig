package org.apache.pig;

/**
 * This interface is used to implement classes that can perform both
 * Load and Store functionalities in a symmetric fashion (thus reversible). 
 * 
 * The symmetry property of implementations is used in the optimization
 * engine therefore violation of this property while implementing this 
 * interface is likely to result in unexpected output from executions.
 * 
 */
public interface ReversibleLoadStoreFunc extends LoadFunc, StoreFunc {

}
