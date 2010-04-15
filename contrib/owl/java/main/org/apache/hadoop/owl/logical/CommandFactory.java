
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

import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;

public class CommandFactory {

    @SuppressWarnings("serial")
    static final Map<String,Class<? extends Command>> COMMANDMAP =
        new HashMap<String,Class<? extends Command>>()
        {
        {
            put("CREATE OWLDATABASE",CreateDatabaseCommand.class);
            put("CREATE OWLTABLE",CreateOwlTableCommand.class);
            put("DESCRIBE OWLTABLE",DescribeOwlTableCommand.class);
            put("DROP OWLDATABASE",DropDatabaseCommand.class);
            put("DROP OWLTABLE",DropOwlTableCommand.class);
            put("PUBLISH DATAELEMENT",PublishDataElementCommand.class);
            put("ALTER OWLTABLE",AlterOwlTableCommand.class);
            put("SELECT DATAELEMENT OBJECTS",SelectDataelementObjectsCommand.class);
            put("SELECT PARTITIONPROPERTY OBJECTS",SelectPartitionpropertyObjectsCommand.class);
            put("SELECT OWLDATABASE OBJECTS",SelectDatabaseObjectsCommand.class);
            put("SELECT OWLTABLE OBJECTS",SelectOwltableObjectsCommand.class);
            put("CREATE GLOBALKEY",CreateGlobalKeyCommand.class);
            put("DROP GLOBALKEY",DropGlobalKeyCommand.class);
            put("SELECT GLOBALKEY OBJECTS",SelectGlobalKeyObjectsCommand.class);
            put("SELECT PARTITION OBJECTS",SelectPartitionObjectsCommand.class);
        }
        };

        public static Command getCommand(String command) throws OwlException {
            if (!COMMANDMAP.containsKey(command.toUpperCase())){
                throw new OwlException(ErrorType.ERROR_UNKNOWN_COMMAND,command);
            }
            Command cmd;
            try {
                cmd = COMMANDMAP.get(command.toUpperCase()).newInstance();
            } catch (InstantiationException e) {
                throw new OwlException(ErrorType.ERROR_COMMAND_INSTANTIATION_FAILURE, command, e);
            } catch (IllegalAccessException e) {
                throw new OwlException(ErrorType.ERROR_COMMAND_INSTANTIATION_FAILURE, command, e);
            }
            return cmd;
        }

}
