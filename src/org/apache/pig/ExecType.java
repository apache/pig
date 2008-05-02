package org.apache.pig;

/**
 * The type of query execution
 */
public enum ExecType {
    /**
     * Run everything on the local machine
     */
    LOCAL,
    /**
     * Use the Hadoop Map/Reduce framework
     */
    MAPREDUCE,
    /**
     * Use the Experimental Hadoop framework; not available yet.
     */
    PIG
}
