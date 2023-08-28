-- When installing via dirac tools, the following is not needed (still here for reference)
--
-- DROP DATABASE IF EXISTS TaskQueueDB;
-- CREATE DATABASE TaskQueueDB;
-- ------------------------------------------------------------------------------
-- Database owner definition
-- USE mysql;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER,REFERENCES ON TaskQueueDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
-- FLUSH PRIVILEGES;

USE TaskQueueDB;
