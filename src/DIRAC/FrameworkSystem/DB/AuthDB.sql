# Everything is created by the DB object upon instantiation if it does not exists.
use AuthDB;

DROP TABLE IF EXISTS `PilotSecrets`;
CREATE TABLE `PilotSecrets` (
  `SecretUUID` VARCHAR(32) NOT NULL,
  `HashedSecret` BINARY(32) NOT NULL,
  `SecretRemainingUseCount` SMALLINT DEFAULT 1,
  `SecretExpirationDate` DATETIME DEFAULT NULL,
  `SecretConstraints` JSON DEFAULT NULL,
  `PilotSecretUseDate` DATETIME DEFAULT NULL,
  PRIMARY KEY (`SecretUUID`),
  UNIQUE KEY `uq_hashed_secret` (`HashedSecret`),
  INDEX `HashedSecret` (`HashedSecret`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
