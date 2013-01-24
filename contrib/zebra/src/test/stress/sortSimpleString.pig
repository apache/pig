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

register /grid/0/dev/hadoopqa/jars/zebra.jar;

--test case : unsort table is /data/all1, sort table is /data/bcookie_sort, sort on bcookie

a1 = LOAD '/data/all1' USING org.apache.hadoop.zebra.pig.TableLoader();
                      
a1order = order a1 by SF_bcookie;

STORE a1order INTO '/data/bcookie_sort' USING org.apache.hadoop.zebra.pig.TableStorer('[SF_bcookie,SF_yuid,SF_ip];[SF_action,SF_afcookie,SF_browser,SF_bucket,SF_cbrn,SF_csc,SF_datestamp,SF_dst_spaceid,SF_dstid,SF_dstpvid,SF_error,SF_match_ts,SF_media,SF_ms,SF_os,SF_pcookie,SF_pg_load_time,SF_pg_size,SF_pg_spaceid,SF_query_term,SF_referrer,SF_server_code,SF_src_spaceid,SF_srcid,SF_srcpvid,SF_timestamp,SF_type,SF_ultspaceid,SF_ydod,MF_demog];[MF_page_params,MF_clickinfo,MLF_viewinfo]');
