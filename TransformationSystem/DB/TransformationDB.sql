# Everything is created by the DB object upon instantiation if it does not exists.
use TransformationDB;

-- This is required to mimic the AUTO_INCREMENT behavior of TaskID which was possible with MyISAM:
CREATE TRIGGER `TaskID_Generator` BEFORE INSERT ON TransformationTasks
FOR EACH ROW SET NEW.TaskID = ( SELECT @last := IFNULL(MAX(TaskID) + 1,1) FROM TransformationTasks WHERE TransformationID=NEW.TransformationID );

