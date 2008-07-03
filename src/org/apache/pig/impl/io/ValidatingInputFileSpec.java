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

package org.apache.pig.impl.io;

import java.io.IOException;

import org.apache.pig.Slicer;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.PigSlicer;
import org.apache.pig.impl.PigContext;

/**
 * Creates a Slicer using its funcSpec in its construction and checks that it's
 * valid.
 */
public class ValidatingInputFileSpec extends FileSpec {

    // Don't send the instantiated slicer over the wire.
    private transient Slicer slicer;

    private static final long serialVersionUID = 1L;

    public ValidatingInputFileSpec(FileSpec fileSpec, DataStorage store)
            throws IOException {
        super(fileSpec.getFileName(), fileSpec.getFuncSpec());
        validate(store);
    }

    /**
     * If the <code>ExecType</code> of <code>context</code> is LOCAL,
     * validation is not performed.
     */
    public ValidatingInputFileSpec(String fileName, String funcSpec,
            PigContext context) throws IOException {

        super(fileName, funcSpec);
        if (context.getExecType() != ExecType.LOCAL) {
            validate(context.getDfs());
        }
    }

    private void validate(DataStorage store) throws IOException {
        getSlicer().validate(store, getFileName());
    }

    /**
     * Returns the Slicer created by this spec's funcSpec.
     */
    public Slicer getSlicer() {
        if (slicer == null) {
            Object loader = PigContext.instantiateFuncFromSpec(getFuncSpec());
            if (loader instanceof Slicer) {
                slicer = (Slicer) loader;
            } else {
                slicer = new PigSlicer(getFuncSpec());
            }
        }
        return slicer;
    }
}
