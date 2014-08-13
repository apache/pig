  avro = LOAD '/data/part-m-00000.avro' USING PigStorage ();
   avro2 = FOREACH avro GENERATE  browser_id, component_version, member_id, page_key, session_id, tracking_time, type;
    fs -rmr testOut/out1;
    STORE avro2 INTO 'testOut/out2'
    USING PigStorage (
    ' {
   "debug": 5,
    "schema":
        { "type":"record","name":"TestRecord",   
          "fields": [ {"name":"browser_id", "type":["null","string"]},  
                      {"name":"component_version","type":"int"},
                      {"name":"member_id","type":"int"},
                      {"name":"page_key","type":["null","string"]},
                      {"name":"session_id","type":"long"},
                      {"name":"tracking_time","type":"long"},
                      {"name":"type","type":["null","string"]}
                   ]
        }
    }
    ');
