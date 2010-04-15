

--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
--   CREATE SEQUENCE CAT_ID_SEQ         START WITH 1 INCREMENT BY 1 NOMAXVALUE;
--   CREATE SEQUENCE OT_ID_SEQ         START WITH 1 INCREMENT BY 1 NOMAXVALUE;
--   CREATE SEQUENCE P_ID_SEQ         START WITH 1 INCREMENT BY 1 NOMAXVALUE;
--   CREATE SEQUENCE DE_ID_SEQ         START WITH 1 INCREMENT BY 1 NOMAXVALUE;
--   CREATE SEQUENCE PAK_ID_SEQ         START WITH 1 INCREMENT BY 1 NOMAXVALUE;
--   CREATE SEQUENCE PRK_ID_SEQ         START WITH 1 INCREMENT BY 1 NOMAXVALUE;
--   CREATE SEQUENCE GK_ID_SEQ         START WITH 1 INCREMENT BY 1 NOMAXVALUE;
--   CREATE SEQUENCE OTKV_ID_SEQ     START WITH 1 INCREMENT BY 1 NOMAXVALUE;
--   CREATE SEQUENCE KV_ID_SEQ         START WITH 1 INCREMENT BY 1 NOMAXVALUE;
--   CREATE SEQUENCE KLV_ID_SEQ         START WITH 1 INCREMENT BY 1 NOMAXVALUE;

CREATE TABLE owl_database(
    odb_id                  NUMBER(10), 
    odb_name                VARCHAR2(255) NOT NULL,
    odb_description         VARCHAR2(255),
    odb_owner               VARCHAR2(255),
    odb_location            VARCHAR2(750) NOT NULL,
    odb_createdat           NUMBER(19,0) NOT NULL,
    odb_lastmodified        NUMBER(19,0) NOT NULL,
    odb_version             NUMBER(10) NOT NULL,
    CONSTRAINT pk_owl_database   PRIMARY KEY (odb_id),
    CONSTRAINT uk_owl_database   UNIQUE  (odb_name),
    CONSTRAINT uk2_owl_database   UNIQUE  (odb_location)
);


CREATE TABLE owl_schema (
    s_id              NUMBER(10),
    s_description     VARCHAR2(255),
    s_owner           VARCHAR2(255),
    s_createdat       NUMBER(19,0),
    s_lastmodified    NUMBER(19,0),
    s_version         NUMBER(10),
    CONSTRAINT pk_owl_schema   PRIMARY KEY   (s_id)
);

CREATE TABLE owl_columnschema (
    cs_id           NUMBER(10),
    cs_sid          NUMBER(10) NOT NULL, -- owlschema in which this key is defined
    cs_name         VARCHAR2(255),
    cs_columntype   VARCHAR2(255),
    cs_subschemaId  NUMBER(10),
    cs_columnnumber NUMBER(10) NOT NULL, -- owlcolumnnumber can NOT be null
    CONSTRAINT pk_owl_columnschema   PRIMARY KEY   (cs_id),
    CONSTRAINT fk_owl_columnschema   FOREIGN KEY   (cs_sid) REFERENCES owl_schema (s_id),
    CONSTRAINT uk_owl_cloumnschema   UNIQUE        (cs_sid, cs_name)
);
CREATE TABLE owl_table (
    ot_id                   NUMBER(10),
    ot_database_id          NUMBER(10) NOT NULL,
    ot_name                 VARCHAR2(255) NOT NULL,
    ot_description          VARCHAR2(255),
    ot_owner                VARCHAR2(255),
    ot_location             VARCHAR2(750),
    ot_createdat            NUMBER(19,0) NOT NULL,
    ot_lastmodified         NUMBER(19,0) NOT NULL,
    ot_version              NUMBER(10) NOT NULL,
    ot_schemaid             NUMBER(10),
    ot_loader               VARCHAR2(255),
    CONSTRAINT pk_owl_table  PRIMARY KEY (ot_id),
    CONSTRAINT fk1_owl_table  FOREIGN KEY (ot_database_id) REFERENCES owl_database (odb_id),
    CONSTRAINT fk2_owl_table  FOREIGN KEY (ot_schemaid) REFERENCES owl_schema (s_id),
    CONSTRAINT uk_owl_table  UNIQUE (ot_database_id, ot_name)
);


CREATE TABLE owl_partition (
    p_id                    NUMBER(10),
    p_owltable_id           NUMBER(10) NOT NULL,
    p_parent_partition_id   NUMBER(10),
    p_description           VARCHAR2(255),
    p_owner                 VARCHAR2(255),
    p_state                 NUMBER(10) NOT NULL, 
    p_isleaf                CHAR(1) NOT NULL,
    p_partition_level       NUMBER(10) NOT NULL,
    p_createdat             NUMBER(19,0) NOT NULL,
    p_lastmodified          NUMBER(19,0) NOT NULL,
    p_version               NUMBER(10) NOT NULL,
    CONSTRAINT pk_owl_partition PRIMARY KEY (p_id),
    CONSTRAINT fk_owl_partition FOREIGN KEY (p_owltable_id) REFERENCES owl_table (ot_id) 
);


CREATE TABLE owl_dataelement (
    de_id                    NUMBER(10), 
    de_owltable_id           NUMBER(10) NOT NULL,
    de_partition_id          NUMBER(10),
    de_location              VARCHAR2(750) NOT NULL, 
    de_description           VARCHAR2(255),
    de_owner                 VARCHAR2(255),
    de_createdat             NUMBER(19,0) NOT NULL,
    de_lastmodified          NUMBER(19,0) NOT NULL,
    de_loader                VARCHAR2(255),
    de_schemaid              NUMBER(10),
    de_version               NUMBER(10) NOT NULL,
    CONSTRAINT pk_owl_dataelement PRIMARY KEY (de_id),
    CONSTRAINT fk1_owl_dataelement FOREIGN KEY (de_owltable_id) REFERENCES owl_table (ot_id), 
    CONSTRAINT fk2_owl_dataelement FOREIGN KEY (de_partition_id) REFERENCES owl_partition (p_id), 
    CONSTRAINT fk3_owl_dataelement FOREIGN KEY (de_schemaid) REFERENCES owl_schema (s_id),
    CONSTRAINT uk_owl_dataelement  UNIQUE (de_owltable_id, de_location)
);


CREATE TABLE owl_partitionkey (
    pak_id                  NUMBER(10),
    pak_owltable_id         NUMBER(10) NOT NULL,
    pak_partition_level     NUMBER(10) NOT NULL,
    pak_name                VARCHAR2(255) NOT NULL,
    pak_data_type           NUMBER(10) NOT NULL,
    pak_partitioning_type   NUMBER(10) NOT NULL,
    pak_interval_start      NUMBER(19,0),
    pak_interval_freq       NUMBER(10),
    pak_interval_freq_unit  NUMBER(10),
    CONSTRAINT pk_owl_partitionkey PRIMARY KEY (pak_id),
    CONSTRAINT fk_owl_partitionkey FOREIGN KEY (pak_owltable_id) REFERENCES owl_table (ot_id), 
    CONSTRAINT uk1_owl_partitionkey UNIQUE (pak_owltable_id, pak_name),
    CONSTRAINT uk2_owl_partitionkey UNIQUE (pak_owltable_id, pak_partition_level)
);


CREATE TABLE owl_propertykey (
    prk_id                  NUMBER(10),
    prk_owltable_id         NUMBER(10) NOT NULL, -- owltable in which this key is defined
    prk_name                VARCHAR2(255) NOT NULL,
    prk_data_type           NUMBER(10) NOT NULL, -- property key data type
    CONSTRAINT pk_owl_propertykey  PRIMARY KEY (prk_id),
    CONSTRAINT fk1_owl_propertykey FOREIGN KEY (prk_owltable_id) REFERENCES owl_table (ot_id), 
    CONSTRAINT uk_owl_propertykey UNIQUE (prk_owltable_id, prk_name)
);


CREATE TABLE owl_globalkey (
    gk_id                   NUMBER(10) ,
    gk_name                 VARCHAR2(255) NOT NULL,
    gk_data_type            NUMBER(10) NOT NULL,
    gk_description          VARCHAR2(255),
    gk_owner                VARCHAR2(255),
    gk_createdat            NUMBER(19,0) NOT NULL,
    gk_lastmodified         NUMBER(19,0) NOT NULL,
    gk_version              NUMBER(10) NOT NULL,
    CONSTRAINT pk_owl_globalkey PRIMARY KEY (gk_id),
    CONSTRAINT uk_owl_globalkey UNIQUE (gk_name)
);


CREATE TABLE owl_tablekeyvalue (
    otkv_id                NUMBER(10) ,
    otkv_owltable_id       NUMBER(10) NOT NULL, -- owltable to which this keyvalue is added
    otkv_prop_key_id       NUMBER(10),
    otkv_glob_key_id       NUMBER(10),
    otkv_int_value         NUMBER(10),
    otkv_string_value      VARCHAR2(255),
    CONSTRAINT pk_owl_tablekeyvalue PRIMARY KEY (otkv_id),
    CONSTRAINT fk1_owl_tablekeyvalue FOREIGN KEY (otkv_owltable_id) REFERENCES owl_table (ot_id), 
    CONSTRAINT fk2_owl_tablekeyvalue FOREIGN KEY (otkv_prop_key_id) REFERENCES owl_propertykey (prk_id), 
    CONSTRAINT fk3_owl_tablekeyvalue FOREIGN KEY (otkv_glob_key_id) REFERENCES owl_globalkey (gk_id) 
);


CREATE TABLE owl_keyvalue (
    kv_id                  NUMBER(10),
    kv_owltable_id         NUMBER(10) NOT NULL, -- owltable to which this keyvalue is added
    kv_partition_id        NUMBER(10), -- partition to which key is added, null if added to owltable
    kv_part_key_id         NUMBER(10),
    kv_prop_key_id         NUMBER(10),
    kv_glob_key_id         NUMBER(10),
    kv_string_value        VARCHAR2(255),
    kv_int_value           NUMBER(10),
    CONSTRAINT pk_owl_keyvalue  PRIMARY KEY (kv_id),
    CONSTRAINT fk1_owl_keyvalue FOREIGN KEY (kv_owltable_id) REFERENCES owl_table (ot_id), 
    CONSTRAINT fk2_owl_keyvalue FOREIGN KEY (kv_partition_id) REFERENCES owl_partition (p_id), 
    CONSTRAINT fk3_owl_keyvalue FOREIGN KEY (kv_part_key_id) REFERENCES owl_partitionkey (pak_id), 
    CONSTRAINT fk4_owl_keyvalue FOREIGN KEY (kv_prop_key_id) REFERENCES owl_propertykey (prk_id), 
    CONSTRAINT fk5_owl_keyvalue FOREIGN KEY (kv_glob_key_id) REFERENCES owl_globalkey (gk_id) 
);


CREATE TABLE owl_keylistvalue (
    klv_id                 NUMBER(10),
    klv_part_key_id        NUMBER(10),
    klv_prop_key_id        NUMBER(10),
    klv_glob_key_id        NUMBER(10),
    klv_string_value       VARCHAR2(255),
    klv_int_value          NUMBER(10),
    CONSTRAINT pk_owl_keylistvalue  PRIMARY KEY (klv_id),
    CONSTRAINT fk1_owl_keylistvalue FOREIGN KEY (klv_part_key_id) REFERENCES owl_partitionkey (pak_id), 
    CONSTRAINT fk2_owl_keylistvalue FOREIGN KEY (klv_prop_key_id) REFERENCES owl_propertykey (prk_id), 
    CONSTRAINT fk3_owl_keylistvalue FOREIGN KEY (klv_glob_key_id) REFERENCES owl_globalkey (gk_id) 
);
