package org.apache.pig.test;

import java.io.IOException;

import org.apache.pig.Slice;
import org.apache.pig.Slicer;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;

/**
 * Makes slices each containing a single value from 0 to value - 1.
 */
public class RangeSlicer
    implements Slicer
{

    /**
     * Expects location to be a Stringified integer, and makes
     * Integer.parseInt(location) slices. Each slice generates a single value,
     * its index in the sequence of slices.
     */
    public Slice[] slice (DataStorage store, String location)
        throws IOException
    {
        int numslices = Integer.parseInt(location);
        Slice[] slices = new Slice[numslices];
        for (int i = 0; i < slices.length; i++) {
            slices[i] = new SingleValueSlice(i);
        }
        return slices;
    }

    public void validate(DataStorage store, String location) throws IOException {
        try {
            Integer.parseInt(location);
        } catch (NumberFormatException nfe) {
            throw new IOException(nfe.getMessage());
        }
    }

    /**
     * A Slice that returns a single value from next.
     */
    public static class SingleValueSlice
        implements Slice
    {
        public int val;

        private transient boolean read;

        public SingleValueSlice (int value)
        {
            this.val = value;
        }

        public void close ()
            throws IOException
        {}

        public long getLength ()
        {
            return 1;
        }

        public String[] getLocations ()
        {
            return new String[0];
        }

        public long getStart() {
            return 0;
        }
        
        public long getPos ()
            throws IOException
        {
            return read ? 1 : 0;
        }

        public float getProgress ()
            throws IOException
        {
            return read ? 1 : 0;
        }

        public void init (DataStorage store)
            throws IOException
        {}

        public boolean next (Tuple value)
            throws IOException
        {
            if (!read) {
                value.appendField(new DataAtom(val));
                read = true;
                return true;
            }
            return false;
        }

        private static final long serialVersionUID = 1L;
    }
}
