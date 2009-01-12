
%Declare var1 '1'
%DECLARE var2 '6'
%declare total '$var1$var2'
aa = load '/data/intermediate/pow/elcarobootstrap/account/full/weekly/data/$date' using PigStorage('\x01');
bb = filter aa by (ARITY == '$total') and ( $4 eq '' or $4 eq 'NULL' or $4 eq 'ss') parallel 400;
a = foreach bb generate $0,$12,$7;

--generate inactive accts
inactiveAccounts = filter a by ($1 neq '') and ($1 == '2') parallel 400;
store inactiveAccounts into '/user/kaleidoscope/pow_stats/20080228/acct/InactiveAcct';
grpInactiveAcct = group inactiveAccounts all;
countInactiveAcct = foreach grpInactiveAcct { generate COUNT( inactiveAccounts ); }
store countInactiveAcct into '/user/kaleidoscope/pow_stats/20080228/acct_stats/InactiveAcctCount';
