\i ~/program-database/ddl/member_tmp.sql
\i ~/program-database/ddl/epsilon_contact_active.sql
\dt+

\echo '  Table epsilon_contact_active - Country BE'
TRUNCATE TABLE epsilon_contact_active;
\copy epsilon_contact_active (email, altid, idtype, country, preferredlanguage, firstname, lastname, birthday, joindate, totalpoints, gender, segmentcode, lastupdatedby) from '/home/9ch7226/epsilon/epsilon_contact_active_BE.csv' DELIMITERS ',' NULL AS '' CSV HEADER;
select count(0) from epsilon_contact_active;

\echo '  Table epsilon_contact_active - Country CZ'
\copy epsilon_contact_active (email, altid, idtype, country, preferredlanguage, firstname, lastname, birthday, joindate, totalpoints, gender, segmentcode, lastupdatedby) from '/home/9ch7226/epsilon/epsilon_contact_active_CZ.csv' DELIMITERS ',' NULL AS '' CSV HEADER;
select count(0) from epsilon_contact_active;

\echo '  Table epsilon_contact_active - Country DE'
\copy epsilon_contact_active (email, altid, idtype, country, preferredlanguage, firstname, lastname, birthday, joindate, totalpoints, gender, segmentcode, lastupdatedby) from '/home/9ch7226/epsilon/epsilon_contact_active_DE.csv' DELIMITERS ',' NULL AS '' CSV HEADER;
select count(0) from epsilon_contact_active;

\echo '  Table epsilon_contact_active - Country ES'
\copy epsilon_contact_active (email, altid, idtype, country, preferredlanguage, firstname, lastname, birthday, joindate, totalpoints, gender, segmentcode, lastupdatedby) from '/home/9ch7226/epsilon/epsilon_contact_active_ES.csv' DELIMITERS ',' NULL AS '' CSV HEADER;
select count(0) from epsilon_contact_active;

\echo '  Table epsilon_contact_active - Country FR'
\copy epsilon_contact_active (email, altid, idtype, country, preferredlanguage, firstname, lastname, birthday, joindate, totalpoints, gender, segmentcode, lastupdatedby) from '/home/9ch7226/epsilon/epsilon_contact_active_FR.csv' DELIMITERS ',' NULL AS '' CSV HEADER;
select count(0) from epsilon_contact_active;

\echo '  Table epsilon_contact_active - Country GB'
\copy epsilon_contact_active (email, altid, idtype, country, preferredlanguage, firstname, lastname, birthday, joindate, totalpoints, gender, segmentcode, lastupdatedby) from '/home/9ch7226/epsilon/epsilon_contact_active_GB.csv' DELIMITERS ',' NULL AS '' CSV HEADER;
select count(0) from epsilon_contact_active;

\echo '  Table epsilon_contact_active - Country IT'
\copy epsilon_contact_active (email, altid, idtype, country, preferredlanguage, firstname, lastname, birthday, joindate, totalpoints, gender, segmentcode, lastupdatedby) from '/home/9ch7226/epsilon/epsilon_contact_active_IT.csv' DELIMITERS ',' NULL AS '' CSV HEADER;
select count(0) from epsilon_contact_active;

\echo '  Table epsilon_contact_active - Country NL'
\copy epsilon_contact_active (email, altid, idtype, country, preferredlanguage, firstname, lastname, birthday, joindate, totalpoints, gender, segmentcode, lastupdatedby) from '/home/9ch7226/epsilon/epsilon_contact_active_NL.csv' DELIMITERS ',' NULL AS '' CSV HEADER;
select count(0) from epsilon_contact_active;

