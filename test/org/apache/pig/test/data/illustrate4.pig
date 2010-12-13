A = load 'test/org/apache/pig/test/data/TestIllustrateInput_invalid.txt' using PigStorage(',')  as (x:int, y:int);
STORE A INTO 'A.txt';
