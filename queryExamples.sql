SELECT * FROM db.consumer_profile_dim LIMIT 100;
SELECT COUNT(*), COUNT(DISTINCT uuid) FROM db.consumer_profile_dim WHERE active_flag=1 AND email_address IN
(SELECT email FROM epsilon.contact WHERE loyaltyzonecode IN ('BEL', 'CZE', 'DEU', 'ESP', 'FRA', 'GBR', 'ITA', 'NLD'));

--SELECT COUNT(*), COUNT(DISTINCT uuid), COUNT(DISTINCT email)
SELECT uuid, email
FROM (SELECT uuid, email, ROW_NUMBER() OVER(PARTITION BY email ORDER BY last_update_ts DESC) AS rownum
FROM (SELECT DISTINCT uuid, lower(email_address) AS email, last_update_ts FROM
(SELECT * FROM db.consumer_profile_dim WHERE active_flag=1)
JOIN hybris.users ON lower(email_address) = lower(p_uid))) WHERE rownum=1;

SELECT uuid, COUNT(email_address) FROM db.consumer_profile_dim GROUP BY uuid HAVING COUNT(email_address) > 1 LIMIT 100;
SELECT uuid, COUNT(DISTINCT email_address) as dcnt FROM db.consumer_profile_dim GROUP BY uuid HAVING dcnt > 1 LIMIT 100;
SELECT email_address, COUNT(DISTINCT uuid) as dcnt FROM db.consumer_profile_dim GROUP BY email_address HAVING dcnt > 1 LIMIT 100;
--Email has multiple IDs and both active
SELECT uuid, email_address, is_disabled, active_flag FROM db.consumer_profile_dim WHERE email_address='exampleEmail1@gmail.com';
--ID has multiple emails but only once active
SELECT uuid, email_address, is_disabled, active_flag FROM db.consumer_profile_dim WHERE uuid='abc123randomid';

UPDATE program.member AS pm SET consumerID = ecp.uuid, lastupdatedat = NOW(), lastupdatedby = 'LSE consumer_profile migration'
FROM program.epsilon_consumer_profile AS ecp WHERE lower(pm.email) = ecp.email AND consumerID IS NULL;

UPDATE program.member AS pm SET consumerID = hcp.uuid, lastupdatedat = NOW(), lastupdatedby = 'LSE consumer_profile migration'
FROM program.hybris_consumer_profile AS hcp WHERE lower(pm.email) = hcp.email AND consumerID IS NULL;
