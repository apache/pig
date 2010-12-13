A = load 'test/org/apache/pig/test/data/TestIllustrateInput.txt'   as (x:int, y:int);
B = FILTER A by x  > 3;
C = FILTER A by x < 3;
store B into 'Bigger';
store C into 'Smaller';
