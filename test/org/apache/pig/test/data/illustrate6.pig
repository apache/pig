a = load 'test/org/apache/pig/test/data/TestIllustrateInput.txt' as (x:int, y:int);
b = group a all;
c = foreach b generate COUNT(a) as count; d = foreach a generate x / c.count; store d into 'test.out';
