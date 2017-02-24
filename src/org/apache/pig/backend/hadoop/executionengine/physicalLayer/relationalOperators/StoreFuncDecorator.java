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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.io.IOException;

import org.apache.pig.ErrorHandling;
import org.apache.pig.ErrorHandler;
import org.apache.pig.PigConfiguration;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

/**
 * This class is used to decorate the {@code StoreFunc#putNext(Tuple)}. It
 * handles errors by calling
 * {@code OutputErrorHandler#handle(String, long, Throwable)} if the
 * {@link StoreFunc} implements {@link ErrorHandling}
 * 
 */
public class StoreFuncDecorator {

    private final StoreFuncInterface storer;
    private final String udfSignature;
    private boolean shouldHandleErrors;
    private ErrorHandler errorHandler;

    public StoreFuncDecorator(StoreFuncInterface storer, String udfSignature) {
        this.storer = storer;
        this.udfSignature = udfSignature;
        init();
    }

    private void init() {
        // The decorators work is mainly on backend only so not creating error
        // handler on frontend
        if (UDFContext.getUDFContext().isFrontend()) {
            return;
        }
        if (storer instanceof ErrorHandling && allowErrors()) {
            errorHandler = ((ErrorHandling) storer).getErrorHandler();
            shouldHandleErrors = true;
        }
    }

    private boolean allowErrors() {
        return UDFContext.getUDFContext().getJobConf()
                .getBoolean(PigConfiguration.PIG_ALLOW_STORE_ERRORS, false);
    }

    /**
     * Call {@code StoreFunc#putNext(Tuple)} and handle errors
     * 
     * @param tuple
     *            the tuple to store.
     * @throws IOException
     */
    public void putNext(Tuple tuple) throws IOException {
        try {
            storer.putNext(tuple);
            if (shouldHandleErrors) {
                errorHandler.onSuccess(udfSignature);
            }
        } catch (Exception e) {
            if (shouldHandleErrors) {
                errorHandler.onError(udfSignature, e, tuple);
            } else {
                throw new IOException(e);
            }
        }
    }

    public StoreFuncInterface getStorer() {
        return storer;
    }
}
