/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.backend.hadoop.executionengine.physicalLayer.util;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.MonitoredUDF;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import com.google.common.base.Function;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * MonitoredUDF is used to watch execution of a UDF, and kill it if the UDF takes an
 * exceedingly long time. Null is returned if the UDF times out.
 *
 * Optionally, UDFs can implement the provided interfaces to provide custom logic for
 * handling errors and default values.
 *
 */
public class MonitoredUDFExecutor implements Serializable {

    private final transient ListeningExecutorService exec;
    private final transient TimeUnit timeUnit;
    private final transient long duration;
    private final transient Object defaultValue;
    @SuppressWarnings("unchecked")
    private final transient EvalFunc evalFunc;
    private final transient Function<Tuple, Object> closure;

    // Let us reflect upon our errors.
    private final transient Class<? extends ErrorCallback> errorCallback;
    private final transient Method errorHandler;
    private final transient Method timeoutHandler;

    @SuppressWarnings("unchecked")
    public MonitoredUDFExecutor(EvalFunc udf) {
        // is 10 enough? This is pretty arbitrary.
        exec = MoreExecutors.listeningDecorator(MoreExecutors.getExitingExecutorService(new ScheduledThreadPoolExecutor(1)));
        this.evalFunc = udf;
        MonitoredUDF anno = udf.getClass().getAnnotation(MonitoredUDF.class);
        timeUnit = anno.timeUnit();
        duration = anno.duration();
        errorCallback = anno.errorCallback();

        // The exceptions really should not happen since our handlers are defined by the parent class which
        // must be extended by all custom handlers.
        try {
            errorHandler = errorCallback.getMethod("handleError", EvalFunc.class, Exception.class);
            timeoutHandler = errorCallback.getMethod("handleTimeout", EvalFunc.class, Exception.class);
        } catch (SecurityException e1) {
            throw new RuntimeException("Unable to use the monitored callback due to a Security Exception while working with "
                    + evalFunc.getClass().getName());
        } catch (NoSuchMethodException e1) {
            throw new RuntimeException("Unable to use the monitored callback because a required method not found while working with "
                    + evalFunc.getClass().getName());
        }

        Type retType = udf.getReturnType();
        defaultValue = getDefaultValue(anno, retType);
        closure = new Function<Tuple, Object>() {
            @Override
            public Object apply(Tuple input) {
                try {
                    return evalFunc.exec(input);
                } catch (IOException e) {
                    // I don't see a CheckedFunction in Guava. Resorting to this hackery.
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private Object getDefaultValue(MonitoredUDF anno, Type retType) {
        if (retType.equals(Integer.TYPE) || retType.equals(Integer.class)) {
            return (anno.intDefault().length == 0) ? null : anno.intDefault()[0];
        } else if (retType.equals(Double.TYPE) || retType.equals(Double.class)) {
            return (anno.doubleDefault().length == 0) ? null : anno.doubleDefault()[0];
        } else if (retType.equals(Float.TYPE) || retType.equals(Float.class)) {
            return (anno.floatDefault().length == 0) ? null : anno.floatDefault()[0];
        } else if (retType.equals(Long.TYPE) || retType.equals(Long.class)) {
            return (anno.longDefault().length == 0) ? null : anno.longDefault()[0];
        } else if (retType.equals(String.class)) {
            return (anno.stringDefault().length == 0) ? null : anno.stringDefault()[0];
        } else {
            // Default default is null.
            return null;
        }
    }

    /**
     * This method *MUST* be called in the finish by POUserFunc.
     * Though we do use an ExitingExecutorService just in case.
     */
    public void terminate() {
        exec.shutdownNow();
    }

    /**
     * UDF authors can optionally extend this class and provide the class of their custom callbacks in the annotation
     * to perform their own handling of errors and timeouts.
     */

    public static class ErrorCallback {

        @SuppressWarnings("unchecked")
        public static void handleError(EvalFunc evalFunc, Exception e) {
            evalFunc.getLogger().error(e);
            StatusReporter reporter = PigStatusReporter.getInstance();
            if (reporter != null &&
                    reporter.getCounter(evalFunc.getClass().getName(), e.toString()) != null) {
                reporter.getCounter(evalFunc.getClass().getName(), e.toString()).increment(1L);
            }
        }

        @SuppressWarnings("unchecked")
        public static void handleTimeout(EvalFunc evalFunc, Exception e) {
            evalFunc.getLogger().error(e);
            StatusReporter reporter = PigStatusReporter.getInstance();
            if (reporter != null &&
                    reporter.getCounter(evalFunc.getClass().getName(), "MonitoredUDF Timeout") != null) {
                reporter.getCounter(evalFunc.getClass().getName(), "MonitoredUDF Timeout").increment(1L);
            }
        }
    }

    public Object monitorExec(final Tuple input) throws IOException {
        CheckedFuture<Object, Exception> f =
            Futures.makeChecked(
                    // the Future whose exceptions we want to catch
                    exec.submit(new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            return closure.apply(input);
                        }
                    }),
                    // How to map those exceptions; we simply rethrow them.
                    // Theoretically we could do some handling of
                    // CancellationException, ExecutionException  and InterruptedException here
                    // and do something special for UDF IOExceptions as opposed to thread exceptions.
                    new Function<Exception, Exception>() {
                        @Override
                        public Exception apply(Exception e) {
                            return e;
                        }
                    });

        Object result = defaultValue;

        // The outer try "should never happen" (tm).
        try {
            try {
                result = f.get(duration, timeUnit);
            } catch (TimeoutException e) {
                timeoutHandler.invoke(null, evalFunc, e);
            } catch (Exception e) {
                errorHandler.invoke(null, evalFunc, e);
            } finally {
                f.cancel(true);
            }
        } catch (IllegalArgumentException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        } catch (InvocationTargetException e) {
            throw new IOException(e);
        }
        return result;
    }
}
