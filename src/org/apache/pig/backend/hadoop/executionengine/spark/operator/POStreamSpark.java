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
package org.apache.pig.backend.hadoop.executionengine.spark.operator;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.data.Tuple;

public class POStreamSpark extends POStream {
	public POStreamSpark(POStream copy) {
		super(copy);
		this.command = copy.getCommand();
		this.executableManagerStr = copy.getExecutableManagerStr();
	}

	public Result getNextTuple(boolean proceed) throws ExecException {
		// The POStream Operator works with ExecutableManager to
		// send input to the streaming binary and to get output
		// from it. To achieve a tuple oriented behavior, two queues
		// are used - one for output from the binary and one for
		// input to the binary. In each getNext() call:
		// 1) If there is no more output expected from the binary, an EOP is
		// sent to successor
		// 2) If there is any output from the binary in the queue, it is passed
		// down to the successor
		// 3) if neither of these two are true and if it is possible to
		// send input to the binary, then the next tuple from the
		// predecessor is got and passed to the binary
		try {
			// if we are being called AFTER all output from the streaming
			// binary has already been sent to us then just return EOP
			// The "allOutputFromBinaryProcessed" flag is set when we see
			// an EOS (End of Stream output) from streaming binary
			if (allOutputFromBinaryProcessed) {
				return new Result(POStatus.STATUS_EOP, null);
			}

			// if we are here AFTER all map() calls have been completed
			// AND AFTER we process all possible input to be sent to the
			// streaming binary, then all we want to do is read output from
			// the streaming binary
			if (allInputFromPredecessorConsumed) {
				Result r = binaryOutputQueue.take();
				if (r.returnStatus == POStatus.STATUS_EOS) {
					// If we received EOS, it means all output
					// from the streaming binary has been sent to us
					// So we can send an EOP to the successor in
					// the pipeline. Also since we are being called
					// after all input from predecessor has been processed
					// it means we got here from a call from close() in
					// map or reduce. So once we send this EOP down,
					// getNext() in POStream should never be called. So
					// we don't need to set any flag noting we saw all output
					// from binary
					r = EOP_RESULT;
				} else if (r.returnStatus == POStatus.STATUS_OK)
					illustratorMarkup(r.result, r.result, 0);
				return (r);
			}

			// if we are here, we haven't consumed all input to be sent
			// to the streaming binary - check if we are being called
			// from close() on the map or reduce
			Result r = getNextHelper((Tuple) null);
			if (isFetchable() || proceed) {
				if (r.returnStatus == POStatus.STATUS_EOP) {
					// we have now seen *ALL* possible input
					// check if we ever had any real input
					// in the course of the map/reduce - if we did
					// then "initialized" will be true. If not, just
					// send EOP down.
					if (getInitialized()) {
						// signal End of ALL input to the Executable Manager's
						// Input handler thread
						binaryInputQueue.put(r);
						// note this state for future calls
						allInputFromPredecessorConsumed = true;
						// look for output from binary
						r = binaryOutputQueue.take();
						if (r.returnStatus == POStatus.STATUS_EOS) {
							// If we received EOS, it means all output
							// from the streaming binary has been sent to us
							// So we can send an EOP to the successor in
							// the pipeline. Also since we are being called
							// after all input from predecessor has been
							// processed
							// it means we got here from a call from close() in
							// map or reduce. So once we send this EOP down,
							// getNext() in POStream should never be called. So
							// we don't need to set any flag noting we saw all
							// output
							// from binary
							r = EOP_RESULT;
						}
					}

				} else if (r.returnStatus == POStatus.STATUS_EOS) {
					// If we received EOS, it means all output
					// from the streaming binary has been sent to us
					// So we can send an EOP to the successor in
					// the pipeline. Also we are being called
					// from close() in map or reduce (this is so because
					// only then this.parentPlan.endOfAllInput is true).
					// So once we send this EOP down, getNext() in POStream
					// should never be called. So we don't need to set any
					// flag noting we saw all output from binary
					r = EOP_RESULT;
				} else if (r.returnStatus == POStatus.STATUS_OK)
					illustratorMarkup(r.result, r.result, 0);
				return r;
			} else {
				// we are not being called from close() - so
				// we must be called from either map() or reduce()
				// get the next Result from helper
				if (r.returnStatus == POStatus.STATUS_EOS) {
					// If we received EOS, it means all output
					// from the streaming binary has been sent to us
					// So we can send an EOP to the successor in
					// the pipeline and also note this condition
					// for future calls
					r = EOP_RESULT;
					allOutputFromBinaryProcessed = true;
				} else if (r.returnStatus == POStatus.STATUS_OK)
					illustratorMarkup(r.result, r.result, 0);
				return r;
			}

		} catch (Exception e) {
			int errCode = 2083;
			String msg = "Error while trying to get next result in POStream.";
			throw new ExecException(msg, errCode, PigException.BUG, e);
		}
	}
}
