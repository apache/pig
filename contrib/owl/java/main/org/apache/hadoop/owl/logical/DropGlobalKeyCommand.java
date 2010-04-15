
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

package org.apache.hadoop.owl.logical;

import java.util.List;

import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.protocol.OwlObject;

public class DropGlobalKeyCommand extends Command {

    String keyName = null;

    public DropGlobalKeyCommand() {
        this.noun = Noun.GLOBALKEY;
        this.verb = Verb.CREATE;
    }

    @Override
    public void addPropertyKey(String keyName, String keyType) throws OwlException {
        this.keyName = OwlUtil.toLowerCase( keyName );
    }

    @Override
    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException{
        if (keyName == null){
            throw new OwlException(ErrorType.ERROR_UNKNOWN_KEY_NAME_OR_TYPE,"Unable to delete global key without key name");
        }

        // Read the global key to see if we're able to select one. 
        // getBackendGlobalKey throws an appropriate exception if it doesn't find a key or more than one match.
        getBackendGlobalKey(backend, keyName);

        // Potential TODO: quick check to see if anything uses it - currently not necessary
        //       as delete will fail if there are any dependencies, and has an 
        //       efficiency to check but could be a surprise later if the backend 
        //       delete ever changes to be recursive. on dependencies. However, 
        //       since that is unlikely and does not make sense to implement, 
        //       leaving as-is for now.

        backend.delete(GlobalKeyEntity.class, "name = \""+keyName+"\"");
        return null;
    }
}
