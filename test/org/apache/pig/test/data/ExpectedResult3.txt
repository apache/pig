
register /data/pigudf.jar;

aa = load '/user/pig/tests/data/singlefile/textdoc.txt ' using PigStorage('\x01');
bb = filter aa by (ARITY == '16') and ( $4 eq '' or $4 eq 'NULL' or $4 eq 'ss') parallel 400;
a = foreach bb generate $0,$12,$7;
store inactiveAccounts into '/user/pig/tests/results/DefineClause_4.out';
