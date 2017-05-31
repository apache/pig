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

import org.apache.pig.ErrorHandler;
import org.apache.pig.ErrorHandling;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

/**
 * This class is used to decorate the {@code LoadFunc#getNext(Tuple)}. It
 * handles errors by calling
 * {@code OutputErrorHandler#handle(String, long, Throwable)} if the
 * {@link LoadFunc} implements {@link ErrorHandling}
 *
 */

public class LoadFuncDecorator {
	private final LoadFunc loader;
    private final String udfSignature;
    private boolean shouldHandleErrors;
    private ErrorHandler errorHandler;

    public LoadFuncDecorator(LoadFunc loader, String udfSignature) {
        this.loader = loader;
        this.udfSignature = udfSignature;
        init();
    }

    private void init() {
        // The decorators work is mainly on backend only so not creating error
        // handler on frontend
        if (UDFContext.getUDFContext().isFrontend()) {
            return;
        }
        if (loader instanceof ErrorHandling && allowErrors()) {
            errorHandler = ((ErrorHandling) loader).getErrorHandler();
            shouldHandleErrors = true;
        }
    }

    private boolean allowErrors() {
        return UDFContext.getUDFContext().getJobConf()
                .getBoolean(PigConfiguration.PIG_ERROR_HANDLING_ENABLED, false);
    }

    /**
     * Call {@code LoadFunc#getNext(Tuple)} and handle errors
     *
     * @throws IOException
     */
    public Tuple getNext() throws IOException {
        Tuple t = null;
        try {
            t = loader.getNext();
            if (shouldHandleErrors) {
                errorHandler.onSuccess(udfSignature);
            }
        } catch (Exception e) {
            if (shouldHandleErrors) {
                errorHandler.onError(udfSignature, e);
            } else {
                throw new IOException(e);
            }
        }
		return t;
    }

    public LoadFunc getLoader() {
        return loader;
    }

    public boolean getErrorHandling() {
        return shouldHandleErrors;
    }
}
