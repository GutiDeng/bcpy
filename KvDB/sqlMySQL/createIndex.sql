DROP PROCEDURE IF EXISTS bcpy_KvDB_createIndex;
/*COMMIT*/
CREATE PROCEDURE `bcpy_KvDB_createIndex` (IN pSchema CHAR(255), IN pTable CHAR(255), IN pName CHAR(255), IN pColumns CHAR(255), IN pIsUnique BOOLEAN)
BEGIN
    IF ((SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = pSchema
            AND TABLE_NAME = pTable
            AND INDEX_NAME = pName) = 0)
    THEN
        SET @sql = CONCAT('ALTER TABLE ', pTable, IF(pIsUnique, ' ADD UNIQUE INDEX ', ' ADD INDEX '), pName, '(', pColumns, ')');
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
    END IF;
END
