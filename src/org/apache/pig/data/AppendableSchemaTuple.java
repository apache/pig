package org.apache.pig.data;

import java.io.File;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.net.URI;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.data.utils.SedesHelper;
import org.apache.pig.data.utils.HierarchyHelper.MustOverride;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;

//TODO need to ensure that serialization and deeserialization is transparent
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class AppendableSchemaTuple<T extends AppendableSchemaTuple> extends SchemaTuple<T> {
    private Tuple append;

    private static final Log LOG = LogFactory.getLog(AppendableSchemaTuple.class);
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public void append(Object val) {
        if (append == null) {
            append = mTupleFactory.newTuple();
        }

        append.append(val);
    }

    protected int appendSize() {
        return append == null ? 0 : append.size();
    }

    protected boolean appendIsNull() {
        return appendSize() == 0;
    }

    protected Object getAppend(int i) throws ExecException {
        return appendIsNull(i) ? null : append.get(i);
    }

    private boolean appendIsNull(int i) throws ExecException {
        return append == null || append.isNull() ? null : append.isNull(i);
    }

    //protected Tuple getAppend() {
    public Tuple getAppend() {
        return append;
    }

    protected void setAppend(Tuple t) {
        append = t;
    }

    private void appendReset() {
        append = null;
    }

    private void setAppend(int fieldNum, Object val) throws ExecException {
        append.set(fieldNum, val);
    }

    //TODO this should account for all of the non-generated objects, and the general cost of being an object
    @MustOverride
    public long getMemorySize() {
        return 0;
    }

    private byte appendType(int i) throws ExecException {
        return append == null ? DataType.UNKNOWN : append.getType(i);
    }

    @MustOverride
    protected SchemaTuple set(SchemaTuple t, boolean checkType) throws ExecException {
        appendReset();
        for (int j = sizeNoAppend(); j < t.size(); j++) {
            append(t.get(j));
        }
        return super.set(t, checkType);
    }

    @MustOverride
    protected SchemaTuple setSpecific(T t) {
        appendReset();
        setAppend(t.getAppend());
        return super.setSpecific(t);
    }

    public SchemaTuple set(List<Object> l) throws ExecException {
        if (l.size() < sizeNoAppend())
            throw new ExecException("Given list of objects has too few fields ("+l.size()+" vs "+sizeNoAppend()+")");

        for (int i = 0; i < sizeNoAppend(); i++)
            set(i, l.get(i));

        appendReset();

        for (int i = sizeNoAppend(); i < l.size(); i++) {
            append(l.get(i++));
        }

        return this;
    }

    @MustOverride
    protected int compareTo(SchemaTuple t, boolean checkType) {
        if (appendSize() > 0) {
            int i;
            int m = sizeNoAppend();
            for (int k = 0; k < size() - sizeNoAppend(); k++) {
                try {
                    i = DataType.compare(getAppend(k), t.get(m++));
                } catch (ExecException e) {
                    throw new RuntimeException("Unable to get append value", e);
                }
                if (i != 0) {
                    return i;
                }
            }
        }
        return 0;
    }

    @MustOverride
    protected int compareToSpecific(T t) {
        int i;
        for (int z = 0; z < appendSize(); z++) {
            try {
                i = DataType.compare(getAppend(z), t.getAppend(z));
            } catch (ExecException e) {
                throw new RuntimeException("Unable to get append", e);
            }
            if (i != 0) {
                return i;
            }
        }
        return 0;
    }

    @MustOverride
    public int hashCode() {
        return append.hashCode();
    }

    @MustOverride
    public void set(int fieldNum, Object val) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            setAppend(diff, val);
            return;
        }
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    @MustOverride
    public Object get(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize())
            return getAppend(diff);
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    @MustOverride
    public boolean isNull(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            return appendIsNull(diff);
        }
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    //TODO: do we even need this?
    @MustOverride
    public void setNull(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            setAppend(diff, null);
        } else {
            throw new ExecException("Invalid index " + fieldNum + " given");
        }
    }

    @MustOverride
    public byte getType(int fieldNum) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            return appendType(diff);
        }
        throw new ExecException("Invalid index " + fieldNum + " given");
    }

    protected void setPrimitiveBase(int fieldNum, Object val, String type) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            setAppend(diff, val);
        }
        throw new ExecException("Given field " + fieldNum + " not a " + type + " field!");
    }

    protected Object getPrimitiveBase(int fieldNum, String type) throws ExecException {
        int diff = fieldNum - sizeNoAppend();
        if (diff < appendSize()) {
            return getAppend(diff);
        }
        throw new ExecException("Given field " + fieldNum + " not a " + type + " field!");
    }

    @MustOverride
    protected void writeElements(DataOutput out) throws IOException {
        if (!appendIsNull()) {
            SedesHelper.writeGenericTuple(out, getAppend());
        }
    }

    @MustOverride
    protected int compareSizeSpecific(T t) {
        int mySz = appendSize();
        int tSz = t.appendSize();
        if (mySz != tSz) {
            return mySz > tSz ? 1 : -1;
        }
        return 0;
    }
}
