-- --------------------------------------------------------------
-- Copyright (c) 2012 Alejandro Escario Méndez
-- 
-- Permission is hereby granted, free of charge, to any
-- person obtaining a copy of this software and associated
-- documentation files (the "Software"), to deal in the
-- Software without restriction, including without limitation
-- the rights to use, copy, modify, merge, publish,
-- distribute, sublicense, and/or sell copies of the
-- Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
-- 
-- The above copyright notice and this permission notice
-- shall be included in all copies or substantial portions of
-- the Software.
-- 
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY
-- KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
-- WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
-- PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
-- OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
-- OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
-- OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
-- SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
-- --------------------------------------------------------------

-- ------------------------------------------------
-- Audit tables
-- ------------------------------------------------

CREATE TABLE IF NOT EXISTS `_aTableAudit` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `table` varchar(45) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `contentId` TEXT NOT NULL COMMENT '	',
  `ip` varchar(46) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL COMMENT '46 chars -> 45 chars of ipv6+nil char',
  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `user` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `userId` int(11) DEFAULT NULL,
  `action` varchar(45) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci AUTO_INCREMENT=1 ;

CREATE TABLE IF NOT EXISTS `_aFieldAudit` (
  `tableAuditId` int(11) NOT NULL,
  `field` varchar(45) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `oldValue` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `newValue` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  PRIMARY KEY (`tableAuditId`,`field`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci AUTO_INCREMENT=1;

-- ------------------------------------------------
-- Audit views
-- ------------------------------------------------

CREATE VIEW `_avFieldAudit` AS 
	SELECT t.`id`, t.`table`, t.`contentId`, t.`ip`, t.`time`, t.`user`, t.`userId`, t.`action`,
			f.`field`, f.`oldValue`, f.`newValue`
		FROM`_aFieldAudit` f
		LEFT JOIN `_aTableAudit` t ON f.`tableAuditId` = t.id;

CREATE VIEW `_avGroupFieldAudit` AS
	SELECT `id`, `table`, `contentId`, `ip`, `time`, `user`, `userId`, `action`, 
			GROUP_CONCAT(DISTINCT `oldValue` SEPARATOR ', ') AS `oldValue`,
			GROUP_CONCAT(DISTINCT `newValue` SEPARATOR ', ') AS `newValue`,
			GROUP_CONCAT(DISTINCT `field` SEPARATOR ', ') AS `field`
		FROM `_avFieldAudit`
		GROUP BY id;


-- ------------------------------------------------
-- Procedures
-- ------------------------------------------------
DELIMITER $$

-- This procedure creates a logger for every table in our database
DROP FUNCTION IF EXISTS `getTriggerSQL`$$
CREATE FUNCTION `getTriggerSQL` ()
	RETURNS TEXT 
	DETERMINISTIC
	READS SQL DATA
BEGIN
	DECLARE done BOOLEAN DEFAULT 0;
	DECLARE tableName TEXT DEFAULT '';
	DECLARE pkNew TEXT DEFAULT '';
	DECLARE pkOld TEXT DEFAULT '';
	DECLARE output TEXT DEFAULT '';
	DECLARE script TEXT DEFAULT '';

	DECLARE curTables CURSOR FOR 
		SELECT `table_name` FROM information_schema.tables 
			WHERE 	`table_schema`=DATABASE() AND 
					`table_name` NOT LIKE '%fieldAudit%' AND
					`table_name`	NOT LIKE '%tableAudit%' AND
					`ENGINE` LIKE 'InnoDB';

	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
	OPEN curTables;

	REPEAT
		FETCH curTables into tableName;
      
		IF NOT done THEN
			SET pkNew = `getConcatPk`(tableName, 'NEW.');
			SET pkOld = `getConcatPk`(tableName, 'OLD.');
			SET output = `getTableAuditScript` (tableName, pkNew, pkOld);

			SET script = CONCAT(script, 
						'\n\n-- ------------------------------\n',
						'-- ', tableName, '\n',
						'-- ------------------------------\n\n',
						output);
		END IF;
	UNTIL done END REPEAT;
    
    CLOSE curTables;

	RETURN script;
END $$

DELIMITER ;

DELIMITER $$

-- This procedure creates a logger for the selected table
DROP FUNCTION IF EXISTS `getTableAuditScript`$$
CREATE FUNCTION `getTableAuditScript` (tableName TEXT,
										pkNew TEXT,
										pkOld TEXT)
	RETURNS TEXT 
	DETERMINISTIC
	READS SQL DATA
BEGIN
	DECLARE done BOOLEAN DEFAULT 0;
	DECLARE ins TEXT DEFAULT '';
	DECLARE temp TEXT DEFAULT '';
	DECLARE output TEXT DEFAULT '';

	DECLARE curFieldUpdate CURSOR FOR      
		 SELECT CONCAT(
			  'IF NOT( OLD.', column_name, ' <=> NEW.', column_name, ') THEN ',
				'INSERT INTO `_aFieldAudit` (`tableAuditId`, `field`, `oldValue`, `newValue`) ',
					'VALUES (auditId, ''', column_name, ''', OLD.', column_name, ', NEW.', column_name, ');',
				'END IF;\n'
				) 
			FROM 
				information_schema.columns 
			WHERE 
				table_schema = DATABASE()
				AND table_name = tableName;

	DECLARE curFieldInsert CURSOR FOR      
		 SELECT CONCAT(
				'INSERT INTO `_aFieldAudit` (`tableAuditId`, `field`, `oldValue`, `newValue`) ',
					'VALUES (auditId, ''', column_name, ''', '''', NEW.', column_name, ');\n'
				) 
			FROM 
				information_schema.columns 
			WHERE 
				table_schema = DATABASE()
				AND table_name = tableName;
  
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=1;


	SET output = 'DELIMITER $\n\n';
	-- -------------------------------------------
	-- Set the insert trigger
	-- -------------------------------------------
	SET temp = CONCAT(
				'DROP TRIGGER IF EXISTS `', tableName, '_insert_audit`$\n',
				'CREATE TRIGGER `', tableName, '_insert_audit`\n',
				'	BEFORE INSERT ON `', tableName, '`\n',
				'	FOR EACH ROW\n',
				'	BEGIN\n',
				'   	DECLARE _now DATETIME DEFAULT  CURRENT_TIMESTAMP;\n',
				'   	DECLARE auditId INT;\n',
				'\n',
				'		INSERT INTO `_aTableAudit` (`table`, `contentId`, `ip`, `time`, `user`, `userId`, `action`)\n',
				'			VALUES(''', tableName, ''', ', pkNew, ', @ip, _now, USER(), @userId, ''INSERT'');\n',
				'\n',
				'		SELECT LAST_INSERT_ID() INTO auditId;\n');

	SET done = 0;
    OPEN curFieldInsert;
    
    REPEAT
      FETCH curFieldInsert into ins;
      IF NOT done THEN
        SET temp = CONCAT(temp, ins, '\n');
      END IF;
    UNTIL done END REPEAT;
    
    CLOSE curFieldInsert;

	SET temp = CONCAT(temp, '	END $\n');

	SET output = CONCAT(output, temp);
	-- -------------------------------------------
	-- Set the update trigger
	-- -------------------------------------------
	SET temp = CONCAT(
				'DROP TRIGGER IF EXISTS `', tableName, '_update_audit`$\n',
				'CREATE TRIGGER `', tableName, '_update_audit`\n',
				'	BEFORE UPDATE ON `', tableName, '`\n',
				'	FOR EACH ROW\n',
				'	BEGIN\n',
				'   	DECLARE _now DATETIME DEFAULT  CURRENT_TIMESTAMP;\n',
				'   	DECLARE auditId INT;\n',
				'\n',
				'		INSERT INTO `_aTableAudit` (`table`, `contentId`, `ip`, `time`, `user`, `userId`, `action`)\n',
				'			VALUES(''', tableName, ''', ', pkOld, ', @ip, _now, USER(), @userId, ''UPDATE'');\n',
				'\n',
				'		SELECT LAST_INSERT_ID() INTO auditId;\n');

	SET done = 0;
    OPEN curFieldUpdate;
    
    REPEAT
      FETCH curFieldUpdate into ins;
      IF NOT done THEN
        SET temp = CONCAT(temp, ins, '\n');
      END IF;
    UNTIL done END REPEAT;
    
    CLOSE curFieldUpdate;

	SET temp = CONCAT(temp, '	END $\n');

	SET output = CONCAT(output, temp);
	-- -------------------------------------------
	-- Set the delete trigger
	-- -------------------------------------------
	SET temp = CONCAT(
				'DROP TRIGGER IF EXISTS `', tableName, '_delete_audit`$\n',
				'CREATE TRIGGER `', tableName, '_delete_audit`\n',
				'	BEFORE DELETE ON `', tableName, '`\n',
				'	FOR EACH ROW\n',
				'	BEGIN\n',
				'   	DECLARE _now DATETIME DEFAULT  CURRENT_TIMESTAMP;\n',
				'\n',
				'		INSERT INTO `_aTableAudit` (`table`, `contentId`, `ip`, `time`, `user`, `userId`, `action`)\n',
				'			VALUES(''', tableName, ''', ', pkOld, ', @ip, _now, USER(), @userId, ''DELETE'');\n',
				'	END $\n');

	SET output = CONCAT(output, temp);
	SET output = CONCAT(output, '\nDELIMITER ;');

	RETURN output;
END $$

DELIMITER ;

-- ------------------------------------------
-- Functions
-- ------------------------------------------
DELIMITER $$

DROP FUNCTION IF EXISTS `getConcatPk`$$
CREATE FUNCTION `getConcatPk` (tableName TEXT, prepend TEXT) 
	RETURNS TEXT 
	DETERMINISTIC
	READS SQL DATA
BEGIN
	DECLARE done BOOLEAN DEFAULT 0;
	DECLARE pk TEXT;
	DECLARE fieldName TEXT DEFAULT '';
	DECLARE curFields CURSOR FOR 
		SELECT `COLUMN_NAME`
		FROM `information_schema`.`COLUMNS`
		WHERE (`TABLE_SCHEMA` = database())
		  AND (`TABLE_NAME` = tableName)
		  AND (`COLUMN_KEY` = 'PRI');
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

	SET pk = '';
	OPEN curFields;

	REPEAT
		FETCH curFields into fieldName;
		IF NOT done THEN
			IF pk = '' THEN
				SET pk = CONCAT(pk, prepend, fieldName);
			ELSE
			 	SET pk = CONCAT(pk, ', ''_'', ',  prepend, fieldName);
			END IF;
		END IF;
	UNTIL done END REPEAT;

    CLOSE curFields;
	
	IF pk = '' THEN
		RETURN '\'\'';
	ELSE
		RETURN CONCAT('CONCAT(', pk, ')');
	END IF;
END $$

DELIMITER ;