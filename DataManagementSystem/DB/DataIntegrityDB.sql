-- -----------------------------------------------------------
-- Integrity database  definition
-- -----------------------------------------------------------

DROP DATABASE IF EXISTS DataIntegrityDB;

CREATE DATABASE DataIntegrityDB;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- Create user DIRAC
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON DataIntegrityDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON DataIntegrityDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';
#FLUSH PRIVILEGES;

-- -----------------------------------------------------------

use DataIntegrityDB;

