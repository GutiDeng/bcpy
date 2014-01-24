DROP PROCEDURE IF EXISTS bcpy_KvDB_lrange;
/*COMMIT*/

CREATE PROCEDURE bcpy_KvDB_lrange(
        IN pTable VARCHAR(128), 
        IN pKey VARCHAR(256), 
        IN pStart INTEGER, 
        IN pStop INTEGER, 
        IN pListLength INTEGER
)

proc_label:BEGIN
    SET @sql = CONCAT('SELECT listStart, listStop INTO @listStart, @listStop',
            ' FROM ', bcpy_KvDB_encTableMeta(pTable), 
            ' WHERE k=\'', pKey,  '\''); 
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    
    IF @listStart IS NULL OR @listStop IS NULL
    THEN
        SET @listStart = 0;
        SET @listStop = 0;
        LEAVE proc_label;
    END IF;
    
    SET @existentStart = @listStart;
    SET @existentStop = IF(@listStop >= @listStart, @listStop, pListLength + @listStop);
    
    SET @wantedStart = IF(pStart >= 0, @existentStart + pStart, @existentStop + 1 + pStart);
    SET @wantedStop = IF(pStop >= 0, @existentStart + pStop, @existentStop + 1 + pStop);
    
    SET @givingStart = GREATEST(@existentStart, @wantedStart) % pListLength;
    SET @givingStop = LEAST(@existentStop, @wantedStop) % pListLength;
    
    IF @givingStart < @givingStop
    THEN
        SET @sql = CONCAT('SELECT v FROM ', bcpy_KvDB_encTableList(pTable), 
                ' WHERE k = \'', pKey, '\' AND f >= ', @givingStart, ' AND f <= ', @givingStop);
    ELSE
        SET @sql = CONCAT('SELECT v FROM ', bcpy_KvDB_encTableList(pTable), 
                ' WHERE k = \'', pKey, '\' AND f >= ', @givingStart, 
                ' UNION ',
                'SELECT v FROM ', bcpy_KvDB_encTableList(pTable), 
                ' WHERE k = \'', pKey, '\' AND f <= ', @givingStop
        );
    END IF;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    
END;
/*COMMIT*/
