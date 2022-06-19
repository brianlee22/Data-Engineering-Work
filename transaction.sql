DROP TABLE IF EXISTS program.transaction;

CREATE TABLE program.transaction (
  id INTEGER PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY (START WITH 1001 INCREMENT BY 1),
  memberId INTEGER,
  benefitId INTEGER,
  country VARCHAR,
  referenceNumber VARCHAR,
  date TIMESTAMP,
  channel VARCHAR,
  store VARCHAR,
  associate VARCHAR,
  monetaryAmount REAL,
  currency VARCHAR,
  points INTEGER,
  expiryDate VARCHAR,
  type VARCHAR,
  createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  lastupdatedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  lastupdatedby VARCHAR
);

CREATE INDEX index_transaction_memberid ON program.transaction(memberid);
CREATE INDEX index_transaction_memberid_benefitid ON program.transaction(memberid, benefitid);
ALTER TABLE program.transaction ADD FOREIGN KEY (memberId) REFERENCES program.member (id);
ALTER TABLE program.transaction ADD FOREIGN KEY (benefitId) REFERENCES program.benefit (id);
ALTER TABLE program.transaction OWNER TO loyalty_owner;
GRANT SELECT ON program.transaction to loyalty_user;
