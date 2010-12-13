A = load 'test/org/apache/pig/test/data/TestIllustrateInput.txt'   as (x:int, y:int);
B = group A by x;
C = foreach B generate group, COUNT(A);
store C into 'out1';
store A into 'out2';
