/**
 * 
 */
package org.apache.pig.test;

import org.apache.pig.builtin.PigStorage;

/**
 * Same as PigStorage with no default constructor - used
 * in testing POCast with a loader function which has no
 * default constructor
 */
public class PigStorageNoDefCtor extends PigStorage {

    /**
     * @param delimiter
     */
    public PigStorageNoDefCtor(String delimiter) {
        super(delimiter);
        // TODO Auto-generated constructor stub
    }

}
