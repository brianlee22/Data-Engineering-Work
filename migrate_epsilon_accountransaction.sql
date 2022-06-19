\i ~/program-database/ddl/member_tmp.sql
\i ~/program-database/ddl/epsilon_accounttransaction.sql
\dt+

\echo '  Table epsilon_accounttransaction - Country BE'
TRUNCATE TABLE epsilon_accounttransaction;
\copy epsilon_accounttransaction (email, altid, country, accounttransactionid, rulesourceactionloyaltyzonecode, accounttransactiondate, accounttransactionvaluevaliditystartdate, accounttransactionvaluevalidityenddate, accounttransactionvalue, accounttransactionvalueavailability, isdeleted, deleteddate, createddate, updateddate, segmentcode, lastupdatedby) from '/home/9ch7226/epsilon/unload_epsilon_accounttransaction_BE.csv' DELIMITERS ',' NULL AS '' CSV HEADER;
select count(0) from epsilon_accounttransaction;

--benefitId? rulesourceactionloyaltyzonecode?

INSERT INTO program.transaction (memberid, country, referencenumber, date, points, expirydate, type, createdat, lastupdatedat, lastupdatedby)
  SELECT a.memberid, e.country, e.accounttransactionid, e.accounttransactiondate, e.accounttransactionvalue, e.accounttransactionvaluevalidityenddate, e.rulesourceactionloyaltyzonecode, e.createddate, e.updateddate, e.lastupdatedby
  FROM epsilon_accounttransaction AS e
  JOIN memberaltids AS a
    ON e.altid = a.altid;
