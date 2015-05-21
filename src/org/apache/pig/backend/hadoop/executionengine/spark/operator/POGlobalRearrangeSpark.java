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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;

/**
 * POGlobalRearrange for spark mode
 */
public class POGlobalRearrangeSpark extends POGlobalRearrange {
    // Use secondary key
    private boolean useSecondaryKey;
    // Sort order for secondary keys;
    private boolean[] secondarySortOrder;

    public POGlobalRearrangeSpark(POGlobalRearrange copy)
            throws ExecException {
        super(copy);
    }

    public boolean isUseSecondaryKey() {
        return useSecondaryKey;
    }

    public void setUseSecondaryKey(boolean useSecondaryKey) {
        this.useSecondaryKey = useSecondaryKey;
    }

    public boolean[] getSecondarySortOrder() {
        return secondarySortOrder;
    }

    public void setSecondarySortOrder(boolean[] secondarySortOrder) {
        this.secondarySortOrder = secondarySortOrder;
    }
}
