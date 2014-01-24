DROP PROCEDURE IF EXISTS bcpy_KvDB_dropIndex;
/*COMMIT*/
CREATE PROCEDURE bcpy_KvDB_dropIndex(in pTable varchar(128), in pIndexName varchar(128) )
BEGIN
    IF((SELECT COUNT(*) AS index_exists FROM information_schema.statistics 
        WHERE TABLE_SCHEMA = DATABASE() and table_name = pTable AND index_name = pIndexName) > 0)
    THEN
        SET @s = CONCAT('DROP INDEX ' , pIndexName , ' ON ' , pTable);
        PREPARE stmt FROM @s;
        EXECUTE stmt;
    END IF;
END;
