/**
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

package org.apache.hadoop.zebra.io;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  TestBasicTable.class,
  TestBasicTableMapSplits.class,
  TestBasicTableProjections.class,
  TestBasicTableSplits.class,
  TestCollection.class,
  TestColumnGroupInserters.class,
  TestColumnGroup.class,
  TestColumnGroupName1.class,
  TestColumnGroupOpen.class,
  TestColumnGroupProjections.class,
  TestColumnGroupReaders.class,
  TestColumnGroupSchemas.class,
  TestColumnGroupSplits.class,
  TestDropColumnGroup.class,
  TestMap.class,
  TestMapOfRecord.class,
  TestMixedType1.class,
  TestNegative.class,
  TestRecord2Map.class,
  TestRecord3Map.class,
  TestRecord.class,
  TestRecordMap.class,
  TestSchema.class,
  TestWrite.class
})

public class TestCheckin {
  // the class remains completely empty, 
  // being used only as a holder for the above annotations
}
