/* Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.pig;

import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.classification.InterfaceAudience;

import java.io.IOException;

@InterfaceAudience.Private
public class DefaultCaster {

    /**
     * This will be used for POCast added for as clauses (PIG-2315)
     * when the CastLineageSetter cannot determine the LoadFunc/EvalFunc to use.
     * @return the {@link LoadCaster}
     * @throws IOException if there is an exception during LoadCaster
     */
    public LoadCaster getLoadCaster() throws IOException {
        return new Utf8StorageConverter();
    }

}
