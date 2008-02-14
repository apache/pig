package org.apache.pig.impl.util;

import java.io.IOException;

/**
 * FIXME PIG-80 replace this code when pig will be java 6 compliant with "throw
 * new IOException(e);"
 */
public class WrappedIOException {

    public static IOException wrap(final Throwable e) {
        return wrap(e.getMessage(), e);
    }
    
    public static IOException wrap(final String message, final Throwable e) {
        final IOException wrappedException = new IOException(message);
        wrappedException.initCause(e);
        return wrappedException;
    }
}
