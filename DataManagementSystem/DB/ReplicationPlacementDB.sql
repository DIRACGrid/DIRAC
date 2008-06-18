--  Schema definition for the ReplicationPlacementDB database - containing DataManagement specific Transformations
--  It simply uses the TransformationDB table definitions.
---
--------------------------------------------------------------------------------
DROP DATABASE IF EXISTS ReplicationPlacementDB;
CREATE DATABASE ReplicationPlacementDB;
--------------------------------------------------------------------------------

-- Database owner definition
USE mysql;

-- Must set passwords for database user by replacing "must_be_set".
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ReplicationPlacementDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ReplicationPlacementDB.* TO Dirac@volhcb03.cern.ch IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ReplicationPlacementDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

-------------------------------------------------------------------------------
USE ReplicationPlacementDB;
-------------------------------------------------------------------------------

SOURCE DIRAC/Core/Transformation/TransformationDB.sql
