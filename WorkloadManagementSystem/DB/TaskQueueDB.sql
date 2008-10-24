-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/DB/TaskQueueDB.sql,v 1.2 2008/10/24 08:34:07 rgracian Exp $

-- ------------------------------------------------------------------------------
--
--  Schema definition for the SandboxDB database - containing the job status
--  history ( logging ) information
--
-- ------------------------------------------------------------------------------

DROP DATABASE IF EXISTS TaskQueueDB;

CREATE DATABASE TaskQueueDB;

-- ------------------------------------------------------------------------------
-- Database owner definition

USE mysql;
DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON TaskQueueDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

USE TaskQueueDB;