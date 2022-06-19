DROP TABLE IF EXISTS program.epsilon_contact_inactive;

CREATE TABLE program.epsilon_contact_inactive (
  email VARCHAR UNIQUE NOT NULL,
  consumerId VARCHAR UNIQUE,
  hybrisId VARCHAR UNIQUE,
  altId VARCHAR,
  idType VARCHAR,
  country VARCHAR NOT NULL,
  preferredLanguage VARCHAR,
  firstName VARCHAR,
  lastName VARCHAR,
  birthday VARCHAR,
  joinDate TIMESTAMP,
  subscribeDate TIMESTAMP,
  status VARCHAR,
  totalPoints INTEGER,
  gender VARCHAR,
  segmentcode VARCHAR,
  crmid_reused BOOLEAN DEFAULT False,
  createdat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  lastupdatedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  lastupdatedby VARCHAR
);

CREATE INDEX index_epsilon_contact_inactive ON program.epsilon_contact_inactive(email);
ALTER TABLE program.epsilon_contact_inactive OWNER TO loyalty_owner;
GRANT SELECT ON program.epsilon_contact_inactive to loyalty_user;
