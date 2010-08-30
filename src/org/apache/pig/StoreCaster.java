package org.apache.pig;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * An interface that provides methods for converting Pig internal types to byte[].
 * It is intended to be used by StoreFunc implementations.
 * @since Pig 0.8
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving // Because we still don't have the map casts quite right
public interface StoreCaster extends LoadCaster {
    public byte[] toBytes(DataBag bag) throws IOException;

    public byte[] toBytes(String s) throws IOException;

    public byte[] toBytes(Double d) throws IOException;

    public byte[] toBytes(Float f) throws IOException;

    public byte[] toBytes(Integer i) throws IOException;

    public byte[] toBytes(Long l) throws IOException;

    public byte[] toBytes(Map<String, Object> m) throws IOException;

    public byte[] toBytes(Tuple t) throws IOException;
    
    public byte[] toBytes(DataByteArray a) throws IOException;
}
