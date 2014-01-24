DROP PROCEDURE IF EXISTS bcpy_KvDB_rpop;
/*COMMIT*/

CREATE PROCEDURE bcpy_KvDB_rpop(
        IN pTable VARCHAR(128),
        IN pKey VARCHAR(256),
        IN pListLength INTEGER
)

proc_label:BEGIN
    DECLARE setting_value TEXT DEFAULT NULL;
    DECLARE CONTINUE HANDLER FOR SQLWARNING BEGIN END;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET setting_value = NULL;
    
    SET @sql = CONCAT('SELECT listStart, listStop INTO @listStart, @listStop ',
            ' FROM ', bcpy_KvDB_encTableMeta(pTable),
            ' WHERE k=\'', pKey,  '\''); 
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    
    IF @listStart IS NULL OR @listStop IS NULL
    THEN
        LEAVE proc_label;
    END IF;
        
    SET @sql = CONCAT('SELECT v FROM ', bcpy_KvDB_encTableList(pTable), 
            ' WHERE k = \'', pKey, '\' AND f = ', @listStop);
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    
    IF @listStart = @listStop
    THEN
        SET @sql = CONCAT('DELETE FROM ', bcpy_KvDB_encTableMeta(pTable), 
                ' WHERE k = \'', pKey, '\'');
    ELSE
        SET @listStop = @listStop - 1;
        SET @listStop = IF(@listStop >= 0, @listStop, pListLength + @listStop);
        SET @sql = CONCAT('INSERT INTO ', bcpy_KvDB_encTableMeta(pTable), 
                ' VALUES (\'', pKey, '\', ', @listStart, ', ', @listStop, ') ',
                ' ON DUPLICATE KEY UPDATE listStart=', @listStart, ', ', 'listStop=', @listStop);
    END IF;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    
END;
/*COMMIT*/
