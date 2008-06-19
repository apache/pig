
aa = load '/data/intermediate/pow/elcarobootstrap/account/full/weekly/data/$date' using PigStorage('\x01');
bb = filter aa by (ARITY == '16') and ( $4 eq '' or $4 eq 'NULL' or $4 eq 'ss') parallel 400;
a = foreach bb generate $0,$12,$7;

--generate inactive accts
%declare destLocation '\'/user/kaleidoscope/pow_stats/$date/acct/InactiveAcct\''
inactiveAccounts = filter a by ($1 neq '') and ($1 == '2') parallel 400;
store inactiveAccounts into $destLocation;
%declare destLocation '\'/user/kaleidoscope/pow_stats/$date/acct_stats/InactiveAcctCount\''
grpInactiveAcct = group inactiveAccounts all;
countInactiveAcct = foreach grpInactiveAcct { generate COUNT( inactiveAccounts ); }
store countInactiveAcct into $destLocation;
