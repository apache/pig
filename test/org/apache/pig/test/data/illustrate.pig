A = load 'test/org/apache/pig/test/data/TestIllustrateInput.txt'   as (x:int, y:int);
B = distinct A;
C = FILTER B by x  > 3;
D = FILTER B by x < 3;
store C into 'Bigger';
store D into 'Smaller';
