

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


connect 'jdbc:derby:derbydb;create=true';

CREATE TABLE owl_database (
    odb_id                  INTEGER,
    odb_name                VARCHAR(255) NOT NULL,
    odb_description         VARCHAR(255),
    odb_owner               VARCHAR(255),
    odb_location            VARCHAR(750) NOT NULL,
    odb_createdat           BIGINT NOT NULL,
    odb_lastmodified        BIGINT NOT NULL,
    odb_version             INTEGER NOT NULL,
    PRIMARY KEY             (odb_id),
    UNIQUE                  (odb_name),
    UNIQUE                  (odb_location)
);

CREATE TABLE owl_schema (
    s_id              INTEGER,
    s_description     VARCHAR(255),
    s_owner           VARCHAR(255),
    s_createdat       BIGINT,
    s_lastmodified    BIGINT,
    s_version         INTEGER,
    PRIMARY KEY       (s_id)
);

CREATE TABLE owl_columnschema (
    cs_id           INTEGER,
    cs_sid          INTEGER NOT NULL, -- owlschema in which this key is defined
    cs_name         VARCHAR(255),
    cs_columntype   VARCHAR(255),
    cs_subschemaId  INTEGER,
    cs_columnnumber INTEGER NOT NULL, -- owlcolumnnumber can NOT be null
    PRIMARY KEY     (cs_id),
    FOREIGN KEY     (cs_sid) REFERENCES owl_schema (s_id),
    UNIQUE          (cs_sid, cs_name)
);

CREATE TABLE owl_table (
    ot_id                   INTEGER,
    ot_database_id          INTEGER NOT NULL,
    ot_name                 VARCHAR(255) NOT NULL,
    ot_description          VARCHAR(255),
    ot_owner                VARCHAR(255),
    ot_location             VARCHAR(750),
    ot_createdat            BIGINT NOT NULL,
    ot_lastmodified         BIGINT NOT NULL,
    ot_version              INTEGER NOT NULL,
    ot_schemaid             INTEGER,
    ot_loader               VARCHAR(255),
    PRIMARY KEY             (ot_id),
    FOREIGN KEY             (ot_database_id) REFERENCES owl_database (odb_id),
    FOREIGN KEY             (ot_schemaid) REFERENCES owl_schema (s_id),
    UNIQUE                  (ot_database_id, ot_name)
);


CREATE TABLE owl_partition (
    p_id                    INTEGER,
    p_owltable_id           INTEGER NOT NULL,
    p_parent_partition_id   INTEGER,
--    p_partition_key_value_id INTEGER NOT NULL,
    p_description           VARCHAR(255),
    p_owner                 VARCHAR(255),
    p_state                 INTEGER NOT NULL, 
    p_isleaf                CHAR(1) NOT NULL,
    p_partition_level       INTEGER NOT NULL,
    p_createdat             BIGINT NOT NULL,
    p_lastmodified          BIGINT NOT NULL,
    p_version               INTEGER NOT NULL,
    PRIMARY KEY             (p_id),
    FOREIGN KEY             (p_owltable_id) REFERENCES owl_table (ot_id) 
);


CREATE TABLE owl_dataelement (
    de_id                    INTEGER,
    de_owltable_id           INTEGER NOT NULL,
    de_partition_id          INTEGER,
    de_location              VARCHAR(750) NOT NULL, 
    de_description           VARCHAR(255),
    de_owner                 VARCHAR(255),
    de_createdat             BIGINT NOT NULL,
    de_lastmodified          BIGINT NOT NULL,
    de_version               INTEGER NOT NULL,
    de_loader                VARCHAR(255),
    de_schemaid              INTEGER,
    PRIMARY KEY              (de_id),
    FOREIGN KEY              (de_owltable_id) REFERENCES owl_table (ot_id), 
    FOREIGN KEY              (de_partition_id) REFERENCES owl_partition (p_id),
    FOREIGN KEY              (de_schemaid) REFERENCES owl_schema (s_id),
    UNIQUE                   (de_owltable_id, de_location)
);


CREATE TABLE owl_partitionkey (
    pak_id                  INTEGER,
    pak_owltable_id         INTEGER NOT NULL,
    pak_partition_level     INTEGER NOT NULL,
    pak_name                VARCHAR(255) NOT NULL,
    pak_data_type           INTEGER NOT NULL,
    pak_partitioning_type   INTEGER NOT NULL,
    pak_interval_start      BIGINT,
    pak_interval_freq       INTEGER,
    pak_interval_freq_unit  INTEGER,
    PRIMARY KEY             (pak_id),
    FOREIGN KEY             (pak_owltable_id) REFERENCES owl_table (ot_id), 
    UNIQUE                  (pak_owltable_id, pak_name),
    UNIQUE                  (pak_owltable_id, pak_partition_level)
);


CREATE TABLE owl_propertykey (
    prk_id                  INTEGER,
    prk_owltable_id         INTEGER NOT NULL, -- owltable in which this key is defined
    prk_name                VARCHAR(255) NOT NULL,
    prk_data_type           INTEGER NOT NULL, -- property key data type
    PRIMARY KEY             (prk_id),
    FOREIGN KEY             (prk_owltable_id) REFERENCES owl_table (ot_id), 
    UNIQUE                  (prk_owltable_id, prk_name)
);


CREATE TABLE owl_globalkey (
    gk_id                   INTEGER,
    gk_name                 VARCHAR(255) NOT NULL,
    gk_data_type            INTEGER NOT NULL,
    gk_description          VARCHAR(255),
    gk_owner                VARCHAR(255),
    gk_createdat            BIGINT NOT NULL,
    gk_lastmodified         BIGINT NOT NULL,
    gk_version              INTEGER NOT NULL,
    PRIMARY KEY             (gk_id),
    UNIQUE                  (gk_name)
);


CREATE TABLE owl_tablekeyvalue (
    otkv_id                INTEGER,
    otkv_owltable_id       INTEGER NOT NULL, -- owltable to which this keyvalue is added
    otkv_prop_key_id       INTEGER,
    otkv_glob_key_id       INTEGER,
    otkv_int_value         INTEGER,
    otkv_string_value      VARCHAR(255),
    PRIMARY KEY            (otkv_id),
    FOREIGN KEY            (otkv_owltable_id) REFERENCES owl_table (ot_id), 
    FOREIGN KEY            (otkv_prop_key_id) REFERENCES owl_propertykey (prk_id), 
    FOREIGN KEY            (otkv_glob_key_id) REFERENCES owl_globalkey (gk_id) 
);


CREATE TABLE owl_keyvalue (
    kv_id                  INTEGER,
    kv_owltable_id         INTEGER NOT NULL, -- owltable to which this keyvalue is added
    kv_partition_id        INTEGER, -- partition to which key is added, null if added to owltable
    kv_part_key_id         INTEGER,
    kv_prop_key_id         INTEGER,
    kv_glob_key_id         INTEGER,
    kv_string_value        VARCHAR(255),
    kv_int_value           INTEGER,
    PRIMARY KEY            (kv_id),
    FOREIGN KEY            (kv_owltable_id) REFERENCES owl_table (ot_id), 
    FOREIGN KEY            (kv_partition_id) REFERENCES owl_partition (p_id), 
    FOREIGN KEY            (kv_part_key_id) REFERENCES owl_partitionkey (pak_id), 
    FOREIGN KEY            (kv_prop_key_id) REFERENCES owl_propertykey (prk_id), 
    FOREIGN KEY            (kv_glob_key_id) REFERENCES owl_globalkey (gk_id) 
);


CREATE TABLE owl_keylistvalue (
    klv_id                 INTEGER,
    klv_part_key_id        INTEGER,
    klv_prop_key_id        INTEGER,
    klv_glob_key_id        INTEGER,
    klv_string_value       VARCHAR(255),
    klv_int_value          INTEGER,
    PRIMARY KEY            (klv_id),
    FOREIGN KEY            (klv_part_key_id) REFERENCES owl_partitionkey (pak_id), 
    FOREIGN KEY            (klv_prop_key_id) REFERENCES owl_propertykey (prk_id), 
    FOREIGN KEY            (klv_glob_key_id) REFERENCES owl_globalkey (gk_id) 
);


