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
package org.apache.pig.impl.builtin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;

/**
 * Currently skipInterval is similar to the randomsampleloader. However, if we were to use an
 * uniform distribution, we could precompute the intervals and read it from a file.
 *
 */
public class PoissonSampleLoader extends SampleLoader {
	
	// Base number of samples needed
	private long baseNumSamples;
	
	/// Count of the map splits
	private static final String MAPSPLITS_COUNT = "pig.mapsplits.count";
	
	/// Conversion factor accounts for the various encodings, compression etc
	private static final String CONV_FACTOR = "pig.inputfile.conversionfactor";
	
	/// For a given mean and a confidence, a sample rate is obtained from a poisson cdf
	private static final String SAMPLE_RATE = "pig.sksampler.samplerate";
	
	/// % of memory available for the input data. This is currenty equal to the memory available
	/// for the skewed join
	private static final String PERC_MEM_AVAIL = "pig.skewedjoin.reduce.memusage";
	
	// 17 is not a magic number. It can be obtained by using a poisson cumulative distribution function with the mean
	// set to 10 (emperically, minimum number of samples) and the confidence set to 95%
	private static final int DEFAULT_SAMPLE_RATE = 17;
	
	// By default the data is multiplied by 2 to account for the encoding
	private static final int DEFAULT_CONV_FACTOR = 2;
	

	public PoissonSampleLoader(String funcSpec, String ns) {
		super(funcSpec);
		super.setNumSamples(Integer.valueOf(ns)); // will be overridden
	}
	
	// n is the number of map tasks
	@Override
	public void setNumSamples(int n) {
		super.setNumSamples(n); // will be overridden
	}
	
	/**
	 * Computes the number of samples for the loader
	 * 
	 * @param inputs : Set to pig inputs
	 * @param pc : PigContext object
	 * 
	 */
	@Override
	public void computeSamples(ArrayList<Pair<FileSpec, Boolean>> inputs, PigContext pc) throws ExecException {
	    
		int numSplits, convFactor, sampleRate;
		Properties pcProps = pc.getProperties();
		
		// Set default values for the various parameters
		try {
			numSplits = Integer.valueOf(pcProps.getProperty(MAPSPLITS_COUNT));
		} catch (NumberFormatException e) {
			String msg = "Couldn't retrieve the number of maps in the job";
			throw new ExecException(msg);
		}
		
		try {
			convFactor = Integer.valueOf(pcProps.getProperty(CONV_FACTOR));
		} catch (NumberFormatException e) {
			convFactor = DEFAULT_CONV_FACTOR;
		}
		
		try {
			sampleRate = Integer.valueOf(pcProps.getProperty(SAMPLE_RATE));
		} catch (NumberFormatException e) {
			sampleRate = DEFAULT_SAMPLE_RATE;
		}
		
		// % of memory available for the records
		float heapPerc = PartitionSkewedKeys.DEFAULT_PERCENT_MEMUSAGE;
        if (pcProps.getProperty(PERC_MEM_AVAIL) != null) {
            try {
                heapPerc = Float.valueOf(pcProps.getProperty(PERC_MEM_AVAIL));
            } catch (NumberFormatException e) {
                // ignore, use default value
            }
        }

        // We are concerned with the size of the first input. In case of globs / directories, 
        // this size is the total size of all the files present in them
        Properties p = UDFContext.getUDFContext().getUDFProperties(SampleLoader.class);
        Long iSize = Long.valueOf((String) p.get("pig.input.0.size"));

        // calculate the base number of samples
		try {
		    float f = (Runtime.getRuntime().maxMemory() * heapPerc) / (float) (iSize * convFactor);
			baseNumSamples = (long) Math.ceil(1.0 / f);
		} catch (ArithmeticException e) {
			int errCode = 1105;
			String msg = "Heap percentage / Conversion factor cannot be set to 0";
			throw new ExecException(msg,errCode,PigException.INPUT);
		}
		
		// set the number of samples
		int n = (int) ((baseNumSamples * sampleRate) / numSplits);
		
		// set the minimum number of samples to 1
		n = (n > 1) ? n : 1;
		setNumSamples(n);

	}
	
	@Override
	public LoadFunc.RequiredFieldResponse fieldsToRead(RequiredFieldList requiredFields) throws FrontendException {
        return new LoadFunc.RequiredFieldResponse(false);
    }

}
