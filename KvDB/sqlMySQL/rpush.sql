DROP PROCEDURE IF EXISTS bcpy_KvDB_rpush;
/*COMMIT*/
CREATE PROCEDURE bcpy_KvDB_rpush(IN pTable varchar(128), IN pKey varchar(256), IN pValue BLOB, IN pListLength INTEGER)
proc_label:BEGIN
    DECLARE rcode INTEGER;
    SET rcode = 0;
    
    SET @sql = CONCAT('SELECT listStart, listStop INTO @listStart, @listStop FROM ', 
            bcpy_KvDB_encTableMeta(pTable), ' WHERE k=\'', pKey,  '\''); 
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    
    IF @listStart IS NULL or @listStop IS NULL
    THEN
        SET @listStart = 0;
        SET @listStop = 0;
    ELSE
        SET @listStop = (pListLength + @listStop + 1) % pListLength;
        IF @listStop = @listStart
        THEN
            SET rcode = -1;
            SELECT rcode;
            LEAVE proc_label;
        END IF;
    END IF;
    
    SET @sql = CONCAT('INSERT INTO ', bcpy_KvDB_encTableList(pTable), ' VALUES (\'', pKey, '\', \'', @listStop, '\', \'', pValue, '\') ',
            ' ON DUPLICATE KEY UPDATE v=\'', pValue, '\'');
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    
    SET @sql = CONCAT('INSERT INTO ', bcpy_KvDB_encTableMeta(pTable), ' VALUES (\'', pKey, '\', ', @listStart, ', ', @listStop, ') ',
            ' ON DUPLICATE KEY UPDATE listStart=', @listStart, ', ', 'listStop=', @listStop);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    
    SELECT rcode;
    
END;
/*COMMIT*/
