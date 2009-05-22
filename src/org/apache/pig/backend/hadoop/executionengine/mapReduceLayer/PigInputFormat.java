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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.data.TargetedTuple;
import org.apache.pig.Slice;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.PigSlicer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.SliceWrapper;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.ValidatingInputFileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;

public class PigInputFormat implements InputFormat<Text, Tuple>,
       JobConfigurable {

    public static final Log LOG = LogFactory
            .getLog(PigInputFormat.class);

    private static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    public static JobConf sJob;

    /**
     * Is the given filename splitable? Usually, true, but if the file is stream
     * compressed, it will not be.
     * 
     * <code>FileInputFormat</code> implementations can override this and
     * return <code>false</code> to ensure that individual input files are
     * never split-up so that {@link Mapper}s process entire files.
     * 
     * @param fs
     *            the file system that the file is on
     * @param filename
     *            the file name to check
     * @return is this file splitable?
     */
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return !filename.getName().endsWith(".gz");
    }

    /**
     * List input directories. Subclasses may override to, e.g., select only
     * files matching a regular expression.
     * 
     * @param job
     *            the job to list input paths for
     * @return array of Path objects
     * @throws IOException
     *             if zero items.
     */
    protected Path[] listPaths(JobConf job) throws IOException {
        Path[] dirs = FileInputFormat.getInputPaths(job);
        if (dirs.length == 0) {
            int errCode = 2092;
            String msg = "No input paths specified in job.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        
        List<Path> result = new ArrayList<Path>();
        for (Path p : dirs) {
            FileSystem fs = p.getFileSystem(job);
            FileStatus[] matches = fs.globStatus(p, hiddenFileFilter);
            for (FileStatus match : matches) {
                result.add(fs.makeQualified(match.getPath()));
            }
        }

        return result.toArray(new Path[result.size()]);
    }

    public void validateInput(JobConf job) throws IOException {
        /*ArrayList<FileSpec> inputs = (ArrayList<FileSpec>) ObjectSerializer
                .deserialize(job.get("pig.inputs"));
        Path[] inputDirs = new Path[inputs.size()];
        int i = 0;
        for (FileSpec spec : inputs) {
            inputDirs[i++] = new Path(spec.getFileName());
        }

        if (inputDirs.length == 0) {
            throw new IOException("No input paths specified in input");
        }

        List<IOException> result = new ArrayList<IOException>();
        int totalFiles = 0;
        for (Path p : inputDirs) {
            FileSystem fs = p.getFileSystem(job);
            if (fs.exists(p)) {
                // make sure all paths are files to avoid exception
                // while generating splits
                for (Path subPath : fs.listPaths(p, hiddenFileFilter)) {
                    FileSystem subFS = subPath.getFileSystem(job);
                    if (!subFS.exists(subPath)) {
                        result.add(new IOException(
                                "Input path does not exist: " + subPath));
                    } else {
                        totalFiles++;
                    }
                }
            } else {
                Path[] paths = fs.globPaths(p, hiddenFileFilter);
                if (paths.length == 0) {
                    result.add(new IOException("Input Pattern " + p
                            + " matches 0 files"));
                } else {
                    // validate globbed paths
                    for (Path gPath : paths) {
                        FileSystem gPathFS = gPath.getFileSystem(job);
                        if (!gPathFS.exists(gPath)) {
                            result.add(new FileNotFoundException(
                                    "Input path doesnt exist : " + gPath));
                        }
                    }
                    totalFiles += paths.length;
                }
            }
        }
        if (!result.isEmpty()) {
            throw new InvalidInputException(result);
        }
        // send output to client.
        LOG.info("Total input paths to process : " + totalFiles);*/
    }
    
    /**
     * Creates input splits one per input and slices of it
     * per DFS block of the input file. Configures the PigSlice
     * and returns the list of PigSlices as an array
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        ArrayList<Pair<FileSpec, Boolean>> inputs;
		ArrayList<ArrayList<OperatorKey>> inpTargets;
		PigContext pigContext;
		try {
			inputs = (ArrayList<Pair<FileSpec, Boolean>>) ObjectSerializer
			        .deserialize(job.get("pig.inputs"));
			inpTargets = (ArrayList<ArrayList<OperatorKey>>) ObjectSerializer
			        .deserialize(job.get("pig.inpTargets"));
			pigContext = (PigContext) ObjectSerializer.deserialize(job
			        .get("pig.pigContext"));
		} catch (Exception e) {
			int errCode = 2094;
			String msg = "Unable to deserialize object.";
			throw new ExecException(msg, errCode, PigException.BUG, e);
		}
        
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
        for (int i = 0; i < inputs.size(); i++) {
            try {
				Path path = new Path(inputs.get(i).first.getFileName());
                                
                                FileSystem fs;
                                
                                try {
                                    fs = path.getFileSystem(job);
                                } catch (Exception e) {
                                    // If an application specific
                                    // scheme was used
                                    // (e.g.: "hbase://table") we will fail
                                    // getting the file system. That's
                                    // ok, we just use the dfs in that case.
                                    fs = new Path("/").getFileSystem(job);
                                }

				// if the execution is against Mapred DFS, set
				// working dir to /user/<userid>
				if(pigContext.getExecType() == ExecType.MAPREDUCE) {
				    fs.setWorkingDirectory(new Path("/user", job.getUser()));
                                }
				
				DataStorage store = new HDataStorage(ConfigurationUtil.toProperties(job));
				ValidatingInputFileSpec spec;
				if (inputs.get(i).first instanceof ValidatingInputFileSpec) {
				    spec = (ValidatingInputFileSpec) inputs.get(i).first;
				} else {
				    spec = new ValidatingInputFileSpec(inputs.get(i).first, store);
				}
				boolean isSplittable = inputs.get(i).second;
				if ((spec.getSlicer() instanceof PigSlicer)) {
				    ((PigSlicer)spec.getSlicer()).setSplittable(isSplittable);
				}
				Slice[] pigs = spec.getSlicer().slice(store, spec.getFileName());
				for (Slice split : pigs) {
				    splits.add(new SliceWrapper(split, pigContext, i, fs, inpTargets.get(i)));
				}
            } catch (ExecException ee) {
            	throw ee;
			} catch (Exception e) {
				int errCode = 2118;
				String msg = "Unable to create input slice for: " + inputs.get(i).first.getFileName();
				throw new ExecException(msg, errCode, PigException.BUG, e);
			}
        }
        return splits.toArray(new SliceWrapper[splits.size()]);
    }

    public RecordReader<Text, Tuple> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter) throws IOException {
        PigInputFormat.sJob = job;
        activeSplit = (SliceWrapper) split;
        return ((SliceWrapper) split).makeReader(job);
    }

    public void configure(JobConf conf) {
    }

    public static SliceWrapper getActiveSplit() {
        return activeSplit;
    }

    private static SliceWrapper activeSplit;
    
}
