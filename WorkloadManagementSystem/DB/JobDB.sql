-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/JobDB.sql,v 1.22 2009/08/26 09:39:53 rgracian Exp $

-- ------------------------------------------------------------------------------
--
--  Schema definition for the JobDB database - the main database of the DIRAC
--  Workload Management System
-- -
-- ------------------------------------------------------------------------------
-- When installing via dirac tools, the following is not needed (still here for reference)
-- 
-- DROP DATABASE IF EXISTS JobDB;
-- CREATE DATABASE JobDB;
-- ------------------------------------------------------------------------------
-- Database owner definition
-- USE mysql;
-- DELETE FROM user WHERE user='Dirac';
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON JobDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
-- FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
USE JobDB;


