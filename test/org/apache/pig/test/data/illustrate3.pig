A = load 'test/org/apache/pig/test/data/TestIllustrateInput.txt'   as (x:int);
A1 = group A by x;
A2 = foreach A1 generate group, COUNT(A);
store A2 into 'A';
B = load 'test/org/apache/pig/test/data/TestIllustrateInput.txt'   as (x:double, y:int);
B1 = group B by x;
B2 = foreach B1 generate group, COUNT(B);
store B2 into 'B';
