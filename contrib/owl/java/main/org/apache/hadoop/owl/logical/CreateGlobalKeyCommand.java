
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
import org.apache.hadoop.owl.entity.PropertyKeyEntity;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlObject;

public class CreateGlobalKeyCommand extends Command{

    private String keyName = null;
    private String keyType = null;

    public CreateGlobalKeyCommand() {
        this.noun = Noun.GLOBALKEY;
        this.verb = Verb.CREATE;
    }

    @Override
    public void addPropertyKey(String keyName, String keyType) throws OwlException{
        this.keyName = OwlUtil.toLowerCase( keyName );
        this.keyType = keyType;
    }

    @Override
    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException{
        if ((keyName == null) || (keyType == null)){
            throw new OwlException(ErrorType.ERROR_UNKNOWN_KEY_NAME_OR_TYPE,"Unable to create global key without key name or type");
        }

        GlobalKeyEntity gkey = new GlobalKeyEntity(keyName, OwlKey.DataType.valueOf(keyType.toUpperCase()).getCode());
        backend.create(gkey);
        return null;
    }
}
