a = load 'test/org/apache/pig/test/data/TestIllustrateInput.txt' as (x:int, y:int);
b = load 'test/org/apache/pig/test/data/TestIllustrateInput2.txt' as (x:int, y:int);
c = join a by x, b by x;
store c into 'test.out';
