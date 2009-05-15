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
package org.apache.pig.backend.executionengine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.Slice;
import org.apache.pig.Slicer;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.util.WrappedIOException;

/**
 * Creates a slice per block size element in all files at location. If location
 * is a glob or a directory, slices are created for every matched file.
 * <p>
 * 
 * If individual files at location end with <code>.gz</code> or
 * <code>.bz2</code>, they will be decompressed before being passed on to the
 * LoadFunc.
 */
public class PigSlicer implements Slicer {
    /**
     * @param funcSpec -
     *                the funcSpec for a LoadFunc that can process the data at
     *                location.
     */
    public PigSlicer(FuncSpec funcSpec) {
        this.funcSpec = funcSpec;        
    }

    public void setSplittable(boolean splittable) {
        this.splittable = splittable;
    }

    public Slice[] slice(DataStorage store, String location) throws IOException {
        validate(store, location);
        List<Slice> slices = new ArrayList<Slice>();
        List<ElementDescriptor> paths = new ArrayList<ElementDescriptor>();

        // If you give a non-glob name, asCollection returns a single
        // element with just that name.
        ElementDescriptor[] globPaths = store.asCollection(location);
        for (int m = 0; m < globPaths.length; m++) {
            paths.add(globPaths[m]);
        }
        for (int j = 0; j < paths.size(); j++) {
            ElementDescriptor fullPath = store.asElement(store
                    .getActiveContainer(), paths.get(j));
            // Skip hadoop's private/meta files ...
            if (fullPath.systemElement()) {
                continue;
            }
            try {
                if (fullPath instanceof ContainerDescriptor) {
                    for (ElementDescriptor child : ((ContainerDescriptor) fullPath)) {
                        paths.add(child);
                    }
                    continue;
                }
            } catch (Exception e) {
                int errCode = 2099;
                String msg = "Problem in constructing slices.";
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
            Map<String, Object> stats = fullPath.getStatistics();
            long bs = (Long) (stats.get(ElementDescriptor.BLOCK_SIZE_KEY));
            long size = (Long) (stats.get(ElementDescriptor.LENGTH_KEY));
            // this hook is mainly for testing, but i'm sure someone can find
            // something fun to do with it
            String bsString = System.getProperty("pig.overrideBlockSize");
            if (bsString != null) {
                bs = Integer.parseInt(bsString);
            }
            long pos = 0;
            String name = fullPath.toString();
            if (name.endsWith(".gz") || !splittable) {
                // Anything that ends with a ".gz" we must process as a complete
                // file
                slices.add(new PigSlice(name, funcSpec, 0, size));
            } else if (size == 0) {
                // add one empty slice.  This is a total hack to deal with the
				// case where hadoop isn't starting maps for empty arrays of
				// InputSplits.  See PIG-619.  This should be removed
				// once we determine why this is.
                slices.add(new PigSlice(name, funcSpec, 0, bs));
            } else {
                while (pos < size) {
                    if (pos + bs > size) {
                        bs = size - pos;
                    }
                    slices.add(new PigSlice(name, funcSpec, pos, bs));
                    pos += bs;
                }
            }
        }
        return slices.toArray(new Slice[slices.size()]);
    }

    public void validate(DataStorage store, String location) throws IOException {
        if (!FileLocalizer.fileExists(location, store)) {
            int errCode = 2100;
            String msg = store.asElement(location) + " does not exist.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
    }

    private FuncSpec funcSpec;
    
    private boolean splittable = true;
}
