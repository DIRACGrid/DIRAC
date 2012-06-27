-- -----------------------------------------------------------
-- Resource Management database definition
-- -----------------------------------------------------------

DROP DATABASE IF EXISTS ResourceManagementDB;
CREATE DATABASE ResourceManagementDB;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- Create user DIRAC
USE mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ResourceManagementDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ResourceManagementDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';

USE ResourceManagementDB;
