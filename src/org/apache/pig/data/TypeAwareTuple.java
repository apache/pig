package org.apache.pig.data;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public interface TypeAwareTuple extends Tuple {

    public void setInt(int idx, int val) throws ExecException;
    public void setFloat(int idx, float val) throws ExecException;
    public void setDouble(int idx, double val) throws ExecException;
    public void setLong(int idx, long val) throws ExecException;
    public void setString(int idx, String val) throws ExecException;
    public void setBoolean(int idx, boolean val) throws ExecException;

    public Integer getInteger(int idx) throws ExecException;
    public Float getFloat(int idx) throws ExecException;
    public Double getDouble(int idx) throws ExecException;
    public Long getLong(int idx) throws ExecException;
    public String getString(int idx) throws ExecException;
    public Boolean getBoolean(int idx) throws ExecException;


}
