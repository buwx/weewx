CREATE TABLE sensor (
  dateTime    BIGINT NOT NULL,
  data        VARCHAR(80),
  description VARCHAR(80),
  INDEX (dateTime)
);

CREATE TABLE last_sensor (
  dateTime BIGINT NOT NULL
);

INSERT INTO last_sensor VALUES(0);
