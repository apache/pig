package org.apache.pig.data;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public interface TypeAwareTuple extends Tuple {

    public void setInt(int idx, int val) throws ExecException;
    public void setFloat(int idx, float val) throws ExecException;
    public void setDouble(int idx, double val) throws ExecException;
    public void setLong(int idx, long val) throws ExecException;
    public void setString(int idx, String val) throws ExecException;
    public void setBoolean(int idx, boolean val) throws ExecException;
    public void setBytes(int idx, byte[] val) throws ExecException;

    // what to do if it is null? should return the primitive type
    public int getInt(int idx) throws ExecException, FieldIsNullException;
    public float getFloat(int idx) throws ExecException, FieldIsNullException;
    public double getDouble(int idx) throws ExecException, FieldIsNullException;
    public long getLong(int idx) throws ExecException, FieldIsNullException;
    public String getString(int idx) throws ExecException, FieldIsNullException;
    public boolean getBoolean(int idx) throws ExecException, FieldIsNullException;
    public byte[] getBytes(int idx) throws ExecException, FieldIsNullException;

    public Schema getSchema();

}
