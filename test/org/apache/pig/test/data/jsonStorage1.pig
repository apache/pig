a = load 'test/org/apache/pig/test/data/jsonStorage1.txt' as (a0:int, a1:{t:(a10:int, a11:chararray)},a2:(a20:double, a21), a3:map[chararray]);
store a into 'jsonStorage1.json' using JsonStorage();
