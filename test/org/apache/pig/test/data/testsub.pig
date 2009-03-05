foo = load '$FILE' as (foo, fast, regenerate);
bar = limit foo $LIMIT;
baz = foreach bar generate $FUNCTION($0);
explain baz;
